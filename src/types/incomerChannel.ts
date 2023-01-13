// Import Internal Dependencies
import {
  DispatcherTransactionMetadata,
  IncomerTransactionMetadata,
  Message
} from "./utils";


// Send by the a Dispatcher

type DispatcherMessages = DispatcherPingMessage;

type DispatcherPingMessage = Message & { data: null, metadata: DispatcherTransactionMetadata };


// Send by an Incomer

type IncomerMessages = IncomerPongMessage | Message & Record<string, any>;

type IncomerPongMessage = Message & { data: null, metadata: IncomerTransactionMetadata };

export type IncomerChannelMessages = {
  IncomerMessage: IncomerMessages;
  DispatcherMessages: DispatcherMessages;
};
