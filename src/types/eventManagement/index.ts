export type Prefix = "test" | "development" | "staging" | "production";

export type EventSubscribe<T extends string = string> = {
  name: T;
  delay?: number;
  horizontalScale?: boolean;
};

export type TransactionMetadata<T extends Instance> = (
  T extends "dispatcher" ?
    { to: string; iteration: number; } :
    { to?: string }
) & {
  origin: string;
  incomerName: string;
  transactionId: string;
  prefix?: Prefix;
  eventTransactionId?: string;
}

export type GenericEvent = {
  name: string;
  data: Record<string, any>;
  [key: string]: any;
};

export type anyFn = (...args: any[]) => void;

export interface RegisteredIncomer {
  providedUUID: string;
  baseUUID: string;
  name: string;
  isDispatcherActiveInstance: boolean;
  lastActivity: number;
  aliveSince: number;
  eventsCast: string[];
  eventsSubscribe: EventSubscribe[];
}

export type Instance = "dispatcher" | "incomer";

