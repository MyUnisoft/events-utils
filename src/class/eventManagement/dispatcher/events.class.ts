// Import Node.js Dependencies
import { EventEmitter } from "node:events";

// Import Third-party Dependencies
import { Channel } from "@myunisoft/redis";
import Ajv, { ValidateFunction } from "ajv";
import type { Logger } from "pino";

// Import Internal Dependencies
import { TransactionStore } from "../../store/transaction.class.js";
import type {
  DispatcherApprovementMessage,
  EventMessage,
  GenericEvent,
  IncomerChannelMessages,
  IncomerRegistrationMessage,
  CloseMessage,
  RetryMessage,
  TransactionMetadata
} from "../../../types/index.js";
import * as eventsSchema from "../../../schema/eventManagement/index.js";
import {
  NestedValidationFunctions,
  type StandardLog,
  type StandardLogOpts,
  concatErrors,
  defaultStandardLog
} from "../../../utils/index.js";

// CONSTANTS
const ajv = new Ajv();
const kDispatcherChannelEvents = ["REGISTER"] as const;

type AnyIncomerChannelMessage<T extends GenericEvent> = (
  IncomerChannelMessages<T>["DispatcherMessages"] | (IncomerChannelMessages<T>["IncomerMessages"] | RetryMessage)
);

type AnyEventChannelMessage<T extends GenericEvent> = IncomerRegistrationMessage | AnyIncomerChannelMessage<T>;

function isIncomerChannelMessage<
  T extends GenericEvent
>(
  event: AnyEventChannelMessage<T>
): event is IncomerChannelMessages<T>["IncomerMessages"] | RetryMessage {
  return kDispatcherChannelEvents.find((message) => message === event.name) === undefined;
}

function isDispatcherChannelMessage<
  T extends GenericEvent
>(
  event: AnyEventChannelMessage<T>
): event is IncomerRegistrationMessage {
  return kDispatcherChannelEvents.find((message) => message === event.name) !== undefined;
}

function isCustomEventMessage<T extends GenericEvent>(
  event: IncomerChannelMessages<T>["IncomerMessages"] | RetryMessage
): event is EventMessage<T> {
  return event.name !== "CLOSE" && event.name !== "RETRY";
}

function isCloseMessage<T extends GenericEvent>(
  event: IncomerChannelMessages<T>["IncomerMessages"] | RetryMessage
): event is CloseMessage {
  return event.name === "CLOSE";
}

function isRetryMessage<T extends GenericEvent>(
  event: IncomerChannelMessages<T>["IncomerMessages"] | RetryMessage
): event is RetryMessage {
  return event.name === "RETRY";
}

type DispatchedEvent<T extends GenericEvent> = (
  Omit<IncomerChannelMessages<T>["DispatcherMessages"] | DispatcherApprovementMessage, "redisMetadata">
) & {
  redisMetadata: Omit<TransactionMetadata<"dispatcher">, "transactionId" | "iteration">
};

export interface DispatchEventOptions<T extends GenericEvent> {
  channel: Channel<IncomerChannelMessages<T>["DispatcherMessages"] | DispatcherApprovementMessage>;
  event: DispatchedEvent<T>;
  redisMetadata: {
    mainTransaction: boolean;
    relatedTransaction?: null | string;
    eventTransactionId?: null | string;
    iteration?: number;
    resolved: boolean;
  };
  store: TransactionStore<"incomer"> | TransactionStore<"dispatcher">;
  dispatcherTransactionUUID?: string;
}

export type customValidationCbFn<T extends GenericEvent> = (event: T) => void;
export type eventsValidationFn<T extends GenericEvent> = Map<string, ValidateFunction<T> | NestedValidationFunctions>;

export interface EventsHandlerOptions<T extends GenericEvent> {
  privateUUID: string;
  dispatcherChannelName: string;
  eventsValidation?: {
    eventsValidationFn?: eventsValidationFn<T>;
    customValidationCbFn?: customValidationCbFn<T>;
  };
  standardLog?: StandardLog<T>;
  parentLogger: Logger;
}

export class EventsHandler<T extends GenericEvent> extends EventEmitter {
  readonly privateUUID: string;
  readonly dispatcherChannelName: string;

  #eventsValidationFn: eventsValidationFn<T>;
  #customValidationCbFn: customValidationCbFn<T> | undefined;

  #logger: Logger;
  #standardLogFn: StandardLog<T>;

  constructor(
    options: EventsHandlerOptions<T>
  ) {
    super();

    Object.assign(this, { ...options });

    this.#logger = options.parentLogger.child({ module: "events-handler" }) || options.parentLogger;
    this.#standardLogFn = options.standardLog ?? defaultStandardLog;

    this.#eventsValidationFn = options.eventsValidation?.eventsValidationFn ?? new Map();
    this.#customValidationCbFn = options.eventsValidation?.customValidationCbFn;

    for (const [eventName, validationSchema] of Object.entries(eventsSchema)) {
      this.#eventsValidationFn.set(eventName, ajv.compile(validationSchema));
    }
  }

  public async dispatch(
    options: DispatchEventOptions<T>
  ): Promise<void> {
    const { channel, store, redisMetadata, event, dispatcherTransactionUUID } = options;

    const transaction = await store.setTransaction({
      ...event,
      redisMetadata: {
        ...event.redisMetadata,
        ...redisMetadata
      } as any
    }, dispatcherTransactionUUID);

    await channel.pub({
      ...event,
      redisMetadata: {
        ...event.redisMetadata,
        iteration: redisMetadata.iteration,
        eventTransactionId: redisMetadata.eventTransactionId,
        transactionId: transaction.redisMetadata.transactionId
      } as any
    });
  }

  public async handleEvents(
    channel: string,
    event: AnyEventChannelMessage<T>
  ) {
    if (channel === this.dispatcherChannelName) {
      if (!isDispatcherChannelMessage(event)) {
        throw new Error("Unknown event on Dispatcher Channel");
      }

      try {
        this.dispatcherChannelMessagesSchemaValidation(event);
      }
      catch (error: any) {
        this.#logger.error(this.#standardLogFn({
          ...event,
          redisMetadata: {
            ...event.redisMetadata,
            origin: this.privateUUID
          }
        } as unknown as StandardLogOpts<T>)(error.stack));
      }

      this.#logger.info(this.#standardLogFn(event as any)("Registration asked"));

      this.emit("APPROVEMENT", event);
    }
    else if (isIncomerChannelMessage(event)) {
      this.incomerChannelMessagesSchemaValidation(event);

      if (isCloseMessage(event)) {
        this.emit("CLOSE", channel, event);
      }
      else if (isRetryMessage(event)) {
        this.emit("RETRY", channel, event);
      }
      else {
        this.emit("CUSTOM_EVENT", channel, event);
      }
    }
    else {
      throw new Error("Unknown event for the given Channel");
    }
  }

  private redisMetadataValidation(
    event: AnyEventChannelMessage<T> | DispatcherApprovementMessage
  ) {
    const { redisMetadata } = event;

    const redisMetadataValidationFn = this.#eventsValidationFn.get("redisMetadata") as ValidateFunction<T>;

    if (!redisMetadataValidationFn(redisMetadata)) {
      throw new Error(
        `Malformed redis metadata: [${concatErrors(redisMetadataValidationFn.errors)}]`
      );
    }
  }


  private dispatcherChannelMessagesSchemaValidation(
    event: IncomerRegistrationMessage | DispatcherApprovementMessage
  ): void {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const { redisMetadata, ...eventRest } = event;

    this.redisMetadataValidation(event);

    const eventValidation = this.#eventsValidationFn.get(eventRest.name) as ValidateFunction<T>;

    if (!eventValidation) {
      throw new Error(`Cannot find the related validation schema for the event: ${event.name}`);
    }

    if (!eventValidation(eventRest)) {
      throw new Error(
        `Malformed event: [${concatErrors(eventValidation.errors)}]`
      );
    }
  }

  private incomerChannelMessagesSchemaValidation(
    event: IncomerChannelMessages<T>["IncomerMessages"] | RetryMessage
  ): void {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const { redisMetadata, ...eventRest } = event;

    this.redisMetadataValidation(event);

    if (event.name === "CLOSE" || event.name === "RETRY") {
      return;
    }

    const eventValidation = this.#eventsValidationFn.get(eventRest.name) as ValidateFunction<T>;
    if (!eventValidation) {
      throw new Error(`Unknown Event ${event.name}`);
    }

    if (this.#customValidationCbFn && isCustomEventMessage(event)) {
      this.#customValidationCbFn({ ...event });

      return;
    }

    if (!eventValidation(eventRest)) {
      throw new Error(
        `Malformed event: [${concatErrors(eventValidation.errors)}]`
      );
    }
  }
}
