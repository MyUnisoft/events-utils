/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable max-lines */
// Import Node.js Dependencies
import { randomUUID } from "node:crypto";

// Import Third-party Dependencies
import { Channel,
  RedisAdapter,
  Types
} from "@myunisoft/redis";
import { Mutex } from "@openally/mutex";
import type { Logger } from "pino";

// Import Internal Dependencies
import {
  TransactionStore,
  type IncomerHandlerTransaction,
  type Transaction,
  type Transactions
} from "../../store/transaction.class.js";
import { IncomerStore } from "../../store/incomer.class.js";
import { IncomerChannelHandler } from "./incomer-channel.class.js";
import {
  type StandardLog,
  type StandardLogOpts,
  defaultStandardLog
} from "../../../utils/index.js";
import { EventsHandler } from "./events.class.js";
import type {
  DispatcherApprovementMessage,
  TransactionMetadata,
  DistributedEventMessage,
  GenericEvent,
  IncomerChannelMessages,
  RegisteredIncomer
} from "../../../types/index.js";
import { DatabaseConnection } from "@myunisoft/redis/dist/types/index.js";


interface DistributeMainTransactionOptions {
  isoPublisherIncomer: RegisteredIncomer;
  incomerTransaction: Transaction<"incomer">;
  inactiveIncomerTransactionStore: TransactionStore<"incomer">;
  incomerTransactionId: string;
}

interface BackupSpreadTransactionsOptions {
  concernedDispatcherTransactions: Transactions<"dispatcher">;
  handlerIncomerTransactions: Transactions<"incomer">;
  inactiveIncomerTransactionStore: TransactionStore<"incomer">;
  incomers: Set<RegisteredIncomer>;
}

interface ResolveTransactions {
  incomers: Set<RegisteredIncomer>;
  backupIncomerTransactions: Transactions<"incomer">;
  dispatcherTransactions: Transactions<"dispatcher">;
}

interface FindISOIncomerOptions {
  incomers: RegisteredIncomer[]
  incomerName: string;
  eventName: string;
  key: "eventsCast" | "eventsSubscribe";
}

type DispatchedEvent<T extends GenericEvent> = (
  IncomerChannelMessages<T>["DispatcherMessages"] | DispatcherApprovementMessage
) & {
  redisMetadata: Omit<DispatcherApprovementMessage["redisMetadata"], "transactionId">
};

export interface DispatchEventOptions<T extends GenericEvent> {
  channel: Channel<DispatchedEvent<T>>;
  redisMetadata: {
    mainTransaction: boolean;
    relatedTransaction?: null | string;
    eventTransactionId?: null | string;
    resolved: boolean;
  }
  event: DispatchedEvent<T>;
}

export type TransactionHandlerOptions<T extends GenericEvent = GenericEvent> = {
  redis: DatabaseConnection<RedisAdapter>;
  eventsHandler: EventsHandler<T>;
  dispatcherTransactionStore: TransactionStore<"dispatcher">;
  backupDispatcherTransactionStore: TransactionStore<"dispatcher">;
  backupIncomerTransactionStore: TransactionStore<"incomer">;
  incomerChannelHandler: IncomerChannelHandler<T>;
  incomerStore: IncomerStore;
  privateUUID: string;
  formattedPrefix: string;
  parentLogger: Logger;
  logger?: Logger;
  standardLog?: StandardLog<T>;
  pingInterval?: number;
};

export interface RedistributeUnresolvedSpreadTransactionOptions {
  backupIncomerTransaction: Transaction<"incomer">;
  isoListener: RegisteredIncomer;
  relatedDispatcherTransactionId: string;
  backupTransactionId: string;
}

export interface RedistributeUnresolvedSpreadTransactionResponse<T extends GenericEvent = GenericEvent> {
  dispatcherTransactionUUID: string;
  event: Omit<DistributedEventMessage, "redisMetadata"> & {
    redisMetadata: Omit<TransactionMetadata<"dispatcher">, "iteration" | "transactionId">
  };
  redisMetadata: DispatchEventOptions<T>["redisMetadata"];
}

export interface RedistributeResolvedSpreadTransactionOptions {
  isoListener: RegisteredIncomer,
  backupIncomerTransaction: Transaction<"incomer">;
  backupTransactionId: string;
  relatedDispatcherTransaction: Transaction<"dispatcher">,
  relatedDispatcherTransactionId: string;
}

export class TransactionHandler<T extends GenericEvent = GenericEvent> {
  readonly prefix: string;
  readonly formattedPrefix: string;
  readonly privateUUID;

  public incomerStore: IncomerStore;
  public dispatcherTransactionStore: TransactionStore<"dispatcher">;
  public backupIncomerTransactionStore: TransactionStore<"incomer">;

  #redis: Types.DatabaseConnection<RedisAdapter>;

  #incomerChannelHandler: IncomerChannelHandler<T>;
  #eventsHandler: EventsHandler<T>;
  #logger: Logger;
  #standardLogFn: StandardLog<T>;

  #resolveTransactionsLock = new Mutex({ concurrency: 1 });

  constructor(opts: TransactionHandlerOptions<T>) {
    Object.assign(this, opts);

    this.#logger = opts.parentLogger.child({ module: "transaction-handler" }) || opts.parentLogger;
    this.#standardLogFn = opts.standardLog ?? defaultStandardLog;
  }

  public async resolveTransactions() {
    const free = await this.#resolveTransactionsLock.acquire();

    try {
      const [incomers, backupIncomerTransactions, dispatcherTransactions] = await Promise.all([
        this.incomerStore.getIncomers(),
        this.backupIncomerTransactionStore.getTransactions(),
        this.dispatcherTransactionStore.getTransactions()
      ]);

      let options = {
        incomers,
        backupIncomerTransactions,
        dispatcherTransactions
      };

      options = await this.handleBackupIncomerTransactions(options);

      options = await this.resolveSpreadTransactions(options);
      await this.resolveMainTransactions(options);
    }
    finally {
      free();
    }
  }

  public async resolveInactiveIncomerTransactions(
    inactiveIncomer: RegisteredIncomer
  ) {
    const inactiveIncomerTransactionStore = new TransactionStore({
      adapter: this.#redis,
      prefix: inactiveIncomer.providedUUID,
      instance: "incomer"
    });

    const incomerTransactions = await inactiveIncomerTransactionStore.getTransactions();
    const dispatcherTransactions = await this.dispatcherTransactionStore.getTransactions();

    const incomers = await this.incomerStore.getIncomers();

    for (const incomer of incomers.values()) {
      if (incomer.providedUUID === inactiveIncomer.providedUUID) {
        incomers.delete(incomer);

        break;
      }
    }

    const transactionsToResolve: Promise<any>[] = [];

    const concernedDispatcherTransactions = [...dispatcherTransactions]
      .filter(([__, dispatcherTransaction]) => dispatcherTransaction.redisMetadata.to === inactiveIncomer.providedUUID ||
        dispatcherTransaction.redisMetadata.to === inactiveIncomer.baseUUID);

    const dispatcherPingTransactions = new Map(
      [...concernedDispatcherTransactions]
        .filter(
          ([__, dispatcherTransaction]) => dispatcherTransaction.name === "PING"
        )
    );

    const incomerPingTransactions = new Map([...incomerTransactions].flatMap(([__, incomerTransaction]) => {
      if (incomerTransaction.name === "PING") {
        return [[incomerTransaction.redisMetadata.relatedTransaction, incomerTransaction]];
      }

      return [];
    }));

    transactionsToResolve.push(
      Promise.all(
        [...dispatcherPingTransactions]
          .map(([relatedPingTransactionId]) => {
            if (!incomerPingTransactions.has(relatedPingTransactionId)) {
              return this.dispatcherTransactionStore.deleteTransaction(relatedPingTransactionId);
            }

            const relatedIncomerPingTransaction = incomerPingTransactions.get(relatedPingTransactionId)!;

            return Promise.all([
              inactiveIncomerTransactionStore.deleteTransaction(relatedIncomerPingTransaction.redisMetadata.transactionId),
              this.dispatcherTransactionStore.deleteTransaction(relatedPingTransactionId)
            ]);
          })
      )
    );

    const dispatcherApprovementTransactions = new Map(
      [...concernedDispatcherTransactions]
        .filter(([__, dispatcherTransaction]) => dispatcherTransaction.name === "APPROVEMENT")
    );

    const incomerRegistrationTransactions = new Map(
      [...incomerTransactions].flatMap(([__, incomerTransaction]) => {
        if (incomerTransaction.name === "REGISTER") {
          return [[incomerTransaction.redisMetadata.relatedTransaction, incomerTransaction]];
        }

        return [];
      })
    );

    transactionsToResolve.push(
      Promise.all(
        [...incomerRegistrationTransactions]
          .map(([relatedApprovementTransactionId, incomerTransaction]) => {
            if (!dispatcherApprovementTransactions.has(relatedApprovementTransactionId)) {
              return inactiveIncomerTransactionStore.deleteTransaction(incomerTransaction.redisMetadata.transactionId);
            }

            return Promise.all([
              inactiveIncomerTransactionStore.deleteTransaction(incomerTransaction.redisMetadata.transactionId),
              this.dispatcherTransactionStore.deleteTransaction(relatedApprovementTransactionId)
            ]);
          })
      )
    );

    await Promise.all(transactionsToResolve);
    transactionsToResolve.length = 0;

    const restIncomerTransaction = new Map(
      [...incomerTransactions]
        .filter(([__, incomerTransaction]) => incomerTransaction.name !== "REGISTER" && incomerTransaction.name !== "PING")
    );

    const concernedDispatcherTransactionRest = new Map(
      [...concernedDispatcherTransactions]
        .filter(([__, dispatcherTransaction]) => dispatcherTransaction.name !== "APPROVEMENT" &&
          dispatcherTransaction.name !== "PING"
        )
    );

    const mainTransactionResolutionPromises = [...restIncomerTransaction.entries()]
      .flatMap(([incomerTransactionId, incomerTransaction]) => {
        if (incomerTransaction.redisMetadata.mainTransaction) {
          const isoPublisherIncomer = this.findISOIncomer({
            incomers: [...incomers.values()],
            incomerName: inactiveIncomer.name,
            eventName: incomerTransaction.name,
            key: "eventsCast"
          });

          return [isoPublisherIncomer ? this.distributeMainTransaction({
            isoPublisherIncomer,
            incomerTransaction,
            inactiveIncomerTransactionStore,
            incomerTransactionId
          }) : this.backupMainTransaction(
            inactiveIncomerTransactionStore,
            incomerTransactionId,
            incomerTransaction
          )];
        }

        return [];
      });

    transactionsToResolve.push(Promise.all(mainTransactionResolutionPromises));

    const handlerIncomerTransactionRest = new Map(
      [...restIncomerTransaction]
        .filter(([__, incomerTransaction]) => !incomerTransaction.redisMetadata.mainTransaction)
    );

    transactionsToResolve.push(this.backupSpreadTransactions({
      concernedDispatcherTransactions: concernedDispatcherTransactionRest,
      handlerIncomerTransactions: handlerIncomerTransactionRest,
      inactiveIncomerTransactionStore,
      incomers
    }));

    await Promise.all(transactionsToResolve);
  }

  private async removeDispatcherUnknownTransaction(dispatcherTransaction: Transaction<"dispatcher">) {
    await this.dispatcherTransactionStore.deleteTransaction(dispatcherTransaction.redisMetadata.transactionId);

    this.#logger.warn(
      this.#standardLogFn({ ...dispatcherTransaction } as any)("Removed Dispatcher Transaction unrelated to an known event")
    );
  }

  private async backupResolvedTransaction(
    relatedHandlerTransaction: Transaction<"incomer">,
    incomerToRemoveTransactionStore: TransactionStore<"incomer">
  ) {
    await Promise.all([
      this.backupIncomerTransactionStore.setTransaction(
        relatedHandlerTransaction,
        relatedHandlerTransaction.redisMetadata.transactionId
      ),
      incomerToRemoveTransactionStore.deleteTransaction(
        relatedHandlerTransaction.redisMetadata.transactionId
      )
    ]);

    this.#logger.debug(
      this.#standardLogFn({ ...relatedHandlerTransaction } as any)("Resolved transaction has been back up")
    );
  }

  private async backupSpreadTransactions(options: BackupSpreadTransactionsOptions): Promise<any> {
    const {
      concernedDispatcherTransactions,
      handlerIncomerTransactions,
      inactiveIncomerTransactionStore,
      incomers
    } = options;

    const toResolve: Promise<any>[] = [];

    for (const [id, dispatcherTransaction] of concernedDispatcherTransactions.entries()) {
      if (!dispatcherTransaction.redisMetadata.relatedTransaction) {
        toResolve.push(this.removeDispatcherUnknownTransaction(dispatcherTransaction));

        continue;
      }

      const isoSubscriberIncomer = this.findISOIncomer({
        incomers: [...incomers],
        incomerName: dispatcherTransaction.redisMetadata.incomerName,
        eventName: dispatcherTransaction.name,
        key: "eventsSubscribe"
      });

      const relatedHandlerTransaction = [...handlerIncomerTransactions.values()]
        .find(
          (transaction) => transaction.redisMetadata.relatedTransaction === dispatcherTransaction.redisMetadata.transactionId
        );

      if (relatedHandlerTransaction && relatedHandlerTransaction.redisMetadata.resolved) {
        toResolve.push(this.backupResolvedTransaction(relatedHandlerTransaction, inactiveIncomerTransactionStore));

        continue;
      }

      if (isoSubscriberIncomer) {
        const { providedUUID } = isoSubscriberIncomer;

        const isoSubscriberChannel = this.#incomerChannelHandler.get(providedUUID) ??
          this.#incomerChannelHandler.set({ uuid: providedUUID });

        toResolve.push(Promise.all([
          this.#eventsHandler.dispatch({
            channel: isoSubscriberChannel,
            store: this.dispatcherTransactionStore,
            redisMetadata: {
              ...dispatcherTransaction.redisMetadata
            },
            event: {
              ...dispatcherTransaction,
              redisMetadata: {
                ...dispatcherTransaction.redisMetadata,
                origin: dispatcherTransaction.redisMetadata.origin,
                to: providedUUID
              }
            } as any
          }),
          this.dispatcherTransactionStore.deleteTransaction(id),
          typeof relatedHandlerTransaction === "undefined" ? () => void 0 :
            inactiveIncomerTransactionStore.deleteTransaction(relatedHandlerTransaction.redisMetadata.transactionId)
        ]));

        this.#logger.info(this.#standardLogFn({
          ...dispatcherTransaction,
          redisMetadata: {
            ...dispatcherTransaction.redisMetadata,
            origin: this.privateUUID,
            to: providedUUID
          }
        } as unknown as StandardLogOpts<T>)("Redistributed unresolved injected event to an Incomer"));

        continue;
      }

      toResolve.push(Promise.all([
        this.backupIncomerTransactionStore.setTransaction({
          ...dispatcherTransaction
        }, dispatcherTransaction.redisMetadata.transactionId),
        typeof relatedHandlerTransaction === "undefined" ? () => void 0 :
          inactiveIncomerTransactionStore.deleteTransaction(relatedHandlerTransaction.redisMetadata.transactionId)
      ]));

      this.#logger.debug(this.#standardLogFn(
        dispatcherTransaction as unknown as StandardLogOpts<T>
      )("Spread transaction has been backup"));

      continue;
    }

    await Promise.all(toResolve);
  }

  private async backupMainTransaction(
    inactiveIncomerTransactionStore: TransactionStore<"incomer">,
    incomerTransactionId: string,
    incomerTransaction: Transaction<"incomer">
  ) {
    const [newlyTransaction] = await Promise.all([
      this.backupIncomerTransactionStore.setTransaction({
        ...incomerTransaction
      }, incomerTransactionId),
      inactiveIncomerTransactionStore.deleteTransaction(incomerTransactionId)
    ]);

    this.#logger.info(this.#standardLogFn({
      ...newlyTransaction,
      redisMetadata: {
        ...newlyTransaction.redisMetadata,
        origin: this.privateUUID
      }
    } as unknown as StandardLogOpts<T>)("Main transaction has been backup"));
  }

  private async distributeMainTransaction(options: DistributeMainTransactionOptions) {
    const {
      isoPublisherIncomer,
      incomerTransaction,
      inactiveIncomerTransactionStore,
      incomerTransactionId
    } = options;
    const { providedUUID } = isoPublisherIncomer;

    const concernedIncomerStore = new TransactionStore({
      adapter: this.#redis,
      prefix: providedUUID,
      instance: "incomer"
    });

    const [newlyIncomerMainTransaction] = await Promise.all([
      concernedIncomerStore.setTransaction({
        ...incomerTransaction,
        redisMetadata: {
          ...incomerTransaction.redisMetadata,
          origin: providedUUID
        }
      }, incomerTransactionId),
      inactiveIncomerTransactionStore.deleteTransaction(incomerTransactionId)
    ]);

    this.#logger.debug(this.#standardLogFn(
      newlyIncomerMainTransaction as unknown as StandardLogOpts<T>
    )("Main transaction redistributed to an Incomer"));
  }

  private async handleBackupIncomerTransactions(options: ResolveTransactions): Promise<ResolveTransactions> {
    const { incomers, backupIncomerTransactions, dispatcherTransactions } = options;

    const toResolve = [];

    for (const [backupTransactionId, backupIncomerTransaction] of backupIncomerTransactions.entries()) {
      if (backupIncomerTransaction.redisMetadata.mainTransaction) {
        const isoPublisher = this.findISOIncomer({
          incomers: [...incomers.values()],
          incomerName: backupIncomerTransaction.redisMetadata.incomerName,
          eventName: backupIncomerTransaction.name,
          key: "eventsCast"
        });

        if (!isoPublisher) {
          continue;
        }

        await this.redistributeMainTransaction(isoPublisher, backupIncomerTransaction, backupTransactionId);

        backupIncomerTransactions.delete(backupTransactionId);

        continue;
      }

      if (backupIncomerTransaction.redisMetadata.relatedTransaction) {
        const isoListener = this.findISOIncomer({
          incomers: [...incomers.values()],
          incomerName: backupIncomerTransaction.redisMetadata.incomerName,
          eventName: backupIncomerTransaction.name,
          key: "eventsSubscribe"
        });

        if (!isoListener) {
          continue;
        }

        const [relatedDispatcherTransactionId, relatedDispatcherTransaction] = [...dispatcherTransactions.entries()]
          .find(
            ([id]) => id === backupIncomerTransaction.redisMetadata.relatedTransaction
          ) || [];

        if (!relatedDispatcherTransactionId) {
          continue;
        }

        if (!backupIncomerTransaction.redisMetadata.resolved) {
          const { dispatcherTransactionUUID, event, redisMetadata } = await this.redistributeUnresolvedSpreadTransaction({
            backupIncomerTransaction,
            isoListener,
            relatedDispatcherTransactionId,
            backupTransactionId
          });

          dispatcherTransactions.set(dispatcherTransactionUUID, {
            ...event,
            redisMetadata: {
              ...event.redisMetadata,
              ...redisMetadata
            }
          } as any);
          backupIncomerTransactions.delete(backupTransactionId);
          dispatcherTransactions.delete(relatedDispatcherTransactionId);

          continue;
        }

        await this.redistributeResolvedSpreadTransaction({
          isoListener,
          backupIncomerTransaction,
          backupTransactionId,
          relatedDispatcherTransaction,
          relatedDispatcherTransactionId
        });

        dispatcherTransactions.set(relatedDispatcherTransactionId, {
          ...relatedDispatcherTransaction,
          redisMetadata: {
            ...relatedDispatcherTransaction.redisMetadata,
            to: isoListener.providedUUID
          }
        } as Transaction<"dispatcher">);
      }
    }

    await Promise.all(toResolve);

    return { incomers, backupIncomerTransactions, dispatcherTransactions };
  }

  private findISOIncomer(options: FindISOIncomerOptions) : RegisteredIncomer | undefined {
    const { incomers, incomerName, eventName, key } = options;

    return incomers.find((incomer) => {
      const isoEvent = key === "eventsCast" ? incomer.eventsCast.find((event) => event === eventName) :
        incomer.eventsSubscribe.find((event) => event.name === eventName);

      return incomer.name === incomerName && isoEvent;
    });
  }

  private async redistributeMainTransaction(
    isoPublisher: RegisteredIncomer,
    backupIncomerTransaction: Transaction<"incomer">,
    backupTransactionId: string
  ): Promise<void> {
    const concernedIncomerStore = new TransactionStore({
      adapter: this.#redis,
      prefix: isoPublisher.providedUUID,
      instance: "incomer"
    });

    await Promise.all([
      concernedIncomerStore.setTransaction({
        ...backupIncomerTransaction,
        redisMetadata: {
          ...backupIncomerTransaction.redisMetadata,
          origin: isoPublisher.providedUUID
        }
      }, backupTransactionId),
      this.backupIncomerTransactionStore.deleteTransaction(backupTransactionId)
    ]);
  }

  private async redistributeUnresolvedSpreadTransaction(options: RedistributeUnresolvedSpreadTransactionOptions):
    Promise<RedistributeUnresolvedSpreadTransactionResponse<T>> {
    const { backupIncomerTransaction, isoListener, relatedDispatcherTransactionId, backupTransactionId } = options;
    const { providedUUID } = isoListener;

    const relatedChannel = this.#incomerChannelHandler.get(providedUUID) ??
      this.#incomerChannelHandler.set({ uuid: providedUUID });

    const dispatcherTransactionUUID = randomUUID();
    const event: Omit<DistributedEventMessage, "redisMetadata"> & {
      redisMetadata: Omit<TransactionMetadata<"dispatcher">, "iteration" | "transactionId">
    } = {
      ...backupIncomerTransaction as IncomerHandlerTransaction["incomerDistributedEventTransaction"],
      redisMetadata: {
        ...backupIncomerTransaction.redisMetadata as
          IncomerHandlerTransaction["incomerDistributedEventTransaction"]["redisMetadata"],
        origin: this.privateUUID,
        to: isoListener.providedUUID
      }
    };

    const redisMetadata = {
      mainTransaction: backupIncomerTransaction.redisMetadata.mainTransaction,
      relatedTransaction: backupIncomerTransaction.redisMetadata.relatedTransaction,
      eventTransactionId: null,
      resolved: backupIncomerTransaction.redisMetadata.resolved
    };

    await Promise.all([
      this.#eventsHandler.dispatch({
        channel: relatedChannel,
        store: this.dispatcherTransactionStore,
        redisMetadata,
        event,
        dispatcherTransactionUUID
      }),
      this.backupIncomerTransactionStore.deleteTransaction(backupTransactionId),
      this.dispatcherTransactionStore.deleteTransaction(relatedDispatcherTransactionId)
    ]);

    return {
      dispatcherTransactionUUID,
      event,
      redisMetadata
    };
  }

  private async redistributeResolvedSpreadTransaction(options: RedistributeResolvedSpreadTransactionOptions) {
    const {
      isoListener,
      backupIncomerTransaction,
      backupTransactionId,
      relatedDispatcherTransaction,
      relatedDispatcherTransactionId
    } = options;

    const relatedStore = new TransactionStore({
      adapter: this.#redis,
      prefix: isoListener.providedUUID,
      instance: "incomer"
    });

    await Promise.all([
      relatedStore.setTransaction({
        ...backupIncomerTransaction,
        redisMetadata: {
          ...backupIncomerTransaction.redisMetadata,
          origin: this.privateUUID,
          to: isoListener.providedUUID
        }
      }, backupIncomerTransaction.redisMetadata.transactionId),
      this.dispatcherTransactionStore.updateTransaction(relatedDispatcherTransactionId, {
        ...relatedDispatcherTransaction,
        redisMetadata: {
          ...relatedDispatcherTransaction.redisMetadata,
          to: isoListener.providedUUID
        }
      } as Transaction<"dispatcher">),
      this.backupIncomerTransactionStore.deleteTransaction(backupTransactionId)
    ]);
  }

  private async resolveSpreadTransactions(options: ResolveTransactions): Promise<ResolveTransactions> {
    const { incomers, backupIncomerTransactions, dispatcherTransactions } = options;

    const toResolve: any[] = [];
    const incomerStateToUpdate = new Set<string>();

    for (const [dispatcherTransactionId, dispatcherTransaction] of dispatcherTransactions.entries()) {
      const relatedIncomer = [...incomers].find((incomer) => incomer.providedUUID === dispatcherTransaction.redisMetadata.to ||
        incomer.baseUUID === dispatcherTransaction.redisMetadata.to);

      const transactionRecipient = relatedIncomer ? relatedIncomer.providedUUID : dispatcherTransaction.redisMetadata.to;

      const [relatedBackupIncomerTransactionId, relatedBackupIncomerTransaction] = [...backupIncomerTransactions.entries()]
        .find(([__, incomerTransaction]) => incomerTransaction.redisMetadata.relatedTransaction === dispatcherTransactionId) ||
          [];

      if (relatedBackupIncomerTransaction) {
        if (relatedBackupIncomerTransaction.redisMetadata.resolved) {
          dispatcherTransaction.redisMetadata.resolved = true;
          toResolve.push(Promise.all([
            this.backupIncomerTransactionStore.deleteTransaction(relatedBackupIncomerTransactionId),
            this.dispatcherTransactionStore.updateTransaction(
              dispatcherTransactionId,
              dispatcherTransaction
            )
          ]));

          backupIncomerTransactions.delete(relatedBackupIncomerTransactionId);
          backupIncomerTransactions.set(dispatcherTransactionId, dispatcherTransaction);

          continue;
        }

        continue;
      }

      if (!relatedIncomer) {
        continue;
      }

      const relatedIncomerTransactionStore = new TransactionStore({
        adapter: this.#redis,
        prefix: transactionRecipient,
        instance: "incomer"
      });

      const relatedIncomerTransactions = await relatedIncomerTransactionStore.getTransactions();

      const filteredIncomerTransactions = [...relatedIncomerTransactions.values()]
        .filter((incomerTransaction) => incomerTransaction.redisMetadata.relatedTransaction === dispatcherTransactionId &&
          incomerTransaction.redisMetadata.resolved) ||
          [];

      // Event not resolved yet
      if (filteredIncomerTransactions.length === 0) {
        continue;
      }

      for (const filteredIncomerTransaction of filteredIncomerTransactions) {
        const filteredIncomerTransactionId = filteredIncomerTransaction.redisMetadata.transactionId!;

        if (dispatcherTransaction.redisMetadata.mainTransaction) {
          // Only in case of ping event
          incomerStateToUpdate.add(filteredIncomerTransaction.redisMetadata.origin);
          toResolve.push(Promise.all([
            relatedIncomerTransactionStore.deleteTransaction(filteredIncomerTransactionId),
            this.dispatcherTransactionStore.deleteTransaction(dispatcherTransactionId)
          ]));

          dispatcherTransactions.delete(dispatcherTransactionId);

          continue;
        }

        if (dispatcherTransaction.name === "APPROVEMENT") {
          if (!filteredIncomerTransaction || !filteredIncomerTransaction.redisMetadata.resolved) {
            continue;
          }

          toResolve.push(Promise.all([
            relatedIncomerTransactionStore.deleteTransaction(filteredIncomerTransactionId),
            this.dispatcherTransactionStore.deleteTransaction(dispatcherTransactionId)
          ]));

          dispatcherTransactions.delete(dispatcherTransactionId);

          continue;
        }

        dispatcherTransaction.redisMetadata.resolved = true;
        incomerStateToUpdate.add((filteredIncomerTransaction.redisMetadata as any).to);
        toResolve.push(Promise.all([
          relatedIncomerTransactionStore.deleteTransaction(filteredIncomerTransactionId),
          this.dispatcherTransactionStore.updateTransaction(
            dispatcherTransactionId,
            dispatcherTransaction
          )
        ]));

        dispatcherTransactions.set(dispatcherTransactionId, dispatcherTransaction);
      }
    }

    toResolve.push([...incomerStateToUpdate.values()].map(
      (incomerId) => this.incomerStore.updateIncomerState(incomerId))
    );

    await Promise.all(toResolve);

    return { incomers, backupIncomerTransactions, dispatcherTransactions };
  }

  private async resolveMainTransactions(options: ResolveTransactions) {
    const { incomers, backupIncomerTransactions, dispatcherTransactions } = options;

    const toResolve: Promise<void>[] = [];
    const incomerStateToUpdate = new Set<string>();

    for (const incomer of incomers) {
      const incomerStore = new TransactionStore({
        adapter: this.#redis,
        prefix: incomer.providedUUID,
        instance: "incomer"
      });

      const discriminatedIncomerTransaction = new Map([...backupIncomerTransactions.entries()].map(([id, backupTransaction]) => {
        const formatted = {
          ...backupTransaction,
          isBackupTransaction: true
        };

        return [id, formatted];
      }));

      const incomerTransactions: Map<string, Transaction<"incomer"> & { isBackupTransaction?: boolean }> = new Map([
        ...await incomerStore.getTransactions(),
        ...discriminatedIncomerTransaction
      ]);

      for (const [incomerTransactionId, incomerTransaction] of incomerTransactions.entries()) {
        if (!incomerTransaction.redisMetadata.mainTransaction) {
          continue;
        }

        const relatedDispatcherTransactions: Transaction<"dispatcher">[] = [...dispatcherTransactions.values()]
          .filter((dispatcherTransaction) => dispatcherTransaction.redisMetadata.relatedTransaction === incomerTransactionId);

        // Event not resolved yet by the dispatcher
        if (relatedDispatcherTransactions.length === 0) {
          continue;
        }

        const unResolvedRelatedTransactions = [...relatedDispatcherTransactions.values()].filter(
          (dispatcherTransaction) => !dispatcherTransaction.redisMetadata.resolved
        );

        // Event not resolved yet by the different incomers
        if (unResolvedRelatedTransactions.length > 0) {
          continue;
        }

        for (const relatedDispatcherTransaction of relatedDispatcherTransactions.values()) {
          if (!incomerTransaction.isBackupTransaction) {
            incomerStateToUpdate.add(relatedDispatcherTransaction.redisMetadata.to);
          }

          toResolve.push(
            this.dispatcherTransactionStore.deleteTransaction(relatedDispatcherTransaction.redisMetadata.transactionId)
          );
        }

        toResolve.push(
          incomerTransaction.isBackupTransaction ? this.backupIncomerTransactionStore.deleteTransaction(incomerTransactionId) :
            incomerStore.deleteTransaction(incomerTransactionId)
        );
      }
    }

    toResolve.push(...[...incomerStateToUpdate.values()].map(
      (incomerId) => this.incomerStore.updateIncomerState(incomerId))
    );

    await Promise.all(toResolve);
  }
}
