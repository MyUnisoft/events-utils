// Import Node.js Dependencies
import { randomUUID } from "node:crypto";

// Import Third-party Dependencies
import {
  KVOptions,
  KVPeer,
  RedisAdapter,
  Types
} from "@myunisoft/redis";

// Import Internal Dependencies
import type {
  TransactionMetadata,
  DispatcherApprovementMessage,
  IncomerRegistrationMessage,
  IncomerChannelMessages,
  DispatcherPingMessage,
  DistributedEventMessage,
  Instance
} from "../../types/index.js";
type BaseTransaction<
  IsMain extends boolean = true,
  RelatedTransaction = IsMain extends true ? null : string
> = {
  published?: boolean;
  mainTransaction: IsMain;
  relatedTransaction: RelatedTransaction;
  resolved: boolean;
};
type HandlerTransaction = BaseTransaction<false> & {
  eventTransactionId?: string;
};

export type DispatcherMainTransaction = DispatcherPingMessage & {
  redisMetadata: BaseTransaction<true> & IncomerChannelMessages["DispatcherMessages"]["redisMetadata"];
}

export interface DispatcherSpreadTransaction {
  dispatcherApprovementTransaction: DispatcherApprovementMessage & {
    redisMetadata: BaseTransaction<false> & DispatcherApprovementMessage["redisMetadata"];
  };
  dispatcherDistributedEventTransaction: DistributedEventMessage & {
    redisMetadata: BaseTransaction<false> & IncomerChannelMessages["DispatcherMessages"]["redisMetadata"];
  };
}

export interface IncomerMainTransaction {
  incomerApprovementTransaction: IncomerRegistrationMessage & {
    redisMetadata: BaseTransaction<true> & IncomerRegistrationMessage["redisMetadata"];
  };
  incomerEventCastTransaction: IncomerChannelMessages["IncomerMessages"] & {
    redisMetadata: BaseTransaction<true> & IncomerChannelMessages["IncomerMessages"]["redisMetadata"];
  };
}

export interface IncomerHandlerTransaction {
  incomerDistributedEventTransaction: IncomerChannelMessages["IncomerMessages"] & {
    redisMetadata: HandlerTransaction & IncomerChannelMessages["IncomerMessages"]["redisMetadata"];
  };
  incomerPongTransaction: DispatcherPingMessage & {
    redisMetadata: HandlerTransaction & DispatcherPingMessage["redisMetadata"];
  };
}

type IncomerTransaction =
  IncomerMainTransaction["incomerApprovementTransaction"] |
  IncomerMainTransaction["incomerEventCastTransaction"] |
  IncomerHandlerTransaction["incomerDistributedEventTransaction"] |
  IncomerHandlerTransaction["incomerPongTransaction"];

type DispatcherTransaction =
  DispatcherSpreadTransaction["dispatcherApprovementTransaction"] |
  DispatcherSpreadTransaction["dispatcherDistributedEventTransaction"] |
  DispatcherMainTransaction;

export type Transaction<T extends Instance> = (
  T extends "dispatcher" ? DispatcherTransaction : IncomerTransaction
) & {
  aliveSince: number;
};

type MetadataWithoutTransactionId<T extends Instance> = T extends "dispatcher"
  ? Omit<TransactionMetadata<"dispatcher">, "to" | "transactionId">
    & { to?: string }
    & (BaseTransaction<true> | BaseTransaction<false>)
  : Omit<TransactionMetadata<"incomer">, "transactionId">
    & (BaseTransaction<true> | HandlerTransaction);

export type PartialTransaction<
  T extends Instance = Instance
> = Omit<Transaction<T>, "redisMetadata" | "aliveSince"> & {
  redisMetadata: MetadataWithoutTransactionId<T>
};

export type Transactions<T extends Instance> = Map<string, Transaction<T>>;

export type TransactionStoreOptions<
  T extends Instance
> = (Partial<KVOptions<Transactions<T>>> &
  T extends "incomer" ? { prefix: string; } : { prefix?: string; }) & {
    adapter: Types.DatabaseConnection<RedisAdapter>;
    instance: T;
};

export class TransactionStore<
  T extends Instance = Instance
> extends KVPeer<
  Transaction<T>,
  null,
  RedisAdapter
> {
  #key: string;

  constructor(
    options: TransactionStoreOptions<T>
  ) {
    super({
      ...options,
      prefix: undefined,
      type: "object"
    });

    this.#key = `${options.prefix ? `${options.prefix}-` : ""}${options.instance}-transaction`;
  }

  #buildTransactionKey(
    transactionId: string
  ): string {
    return `${this.#key}-${transactionId}`;
  }

  async* transactionLazyFetch(count = 5000) {
    let cursor = 0;

    do {
      const [lastCursor, elements] = await this.adapter.scan(
        cursor,
        "MATCH",
        `${this.#key}-*`,
        "COUNT",
        count
      );

      cursor = Number(lastCursor);
      yield elements;

      continue;
    }
    while (cursor !== 0);
  }

  async getTransactions(): Promise<Transactions<T>> {
    const mappedTransactions: Transactions<T> = new Map();

    for await (const transactionKeys of this.transactionLazyFetch()) {
      const transactions = await Promise.all(transactionKeys.map(
        (transactionKey) => this.getValue(transactionKey)
      ));

      for (const transaction of transactions) {
        if (
          transaction !== null &&
          (transaction.redisMetadata && "transactionId" in transaction.redisMetadata)
        ) {
          mappedTransactions.set(transaction.redisMetadata.transactionId!, transaction);
        }
      }
    }

    return mappedTransactions;
  }

  async setTransaction(
    transaction: PartialTransaction<T>,
    transactionId: string = randomUUID()
  ): Promise<Transaction<T>> {
    const formattedTransaction = {
      ...transaction,
      redisMetadata: {
        ...transaction.redisMetadata,
        eventTransactionId: transaction.redisMetadata.eventTransactionId ?? transactionId,
        transactionId
      },
      aliveSince: Date.now()
    } as unknown as Transaction<T>;

    await this.setValue({
      key: this.#buildTransactionKey(transactionId),
      value: formattedTransaction
    });

    return formattedTransaction;
  }

  async updateTransaction(
    transactionId: string,
    transaction: Transaction<T>
  ): Promise<void> {
    this.setValue({
      key: this.#buildTransactionKey(transactionId),
      value: {
        ...transaction,
        aliveSince: Date.now()
      }
    });
  }

  getTransactionById(
    transactionId: string
  ): Promise<Transaction<T> | null> {
    return this.getValue(
      this.#buildTransactionKey(transactionId)
    );
  }

  async deleteTransaction(
    transactionId: string
  ): Promise<void> {
    await this.deleteValue(
      this.#buildTransactionKey(transactionId)
    );
  }

  async deleteTransactions(
    transactionIds: string[]
  ): Promise<void> {
    await this.adapter.del(transactionIds);
  }
}
