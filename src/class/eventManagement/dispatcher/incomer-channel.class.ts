// Import Third-party Dependencies
import {
  Channel,
  RedisAdapter,
  Types
} from "@myunisoft/redis";
import type { Logger } from "pino";

// Import Internal Dependencies
import type {
  DistributedEventMessage,
  GenericEvent,
  IncomerChannelMessages
} from "../../../types/index.js";

export interface IncomerChannelHandlerOptions<T extends GenericEvent> {
  logger: Logger;
  channels?: Map<string, Channel<DistributedEventMessage<T>>>;
}

export interface SetChannelOptions {
  uuid: string;
  subscribe?: boolean;
}

export type ChannelMessages<T extends GenericEvent> = Channel<
  IncomerChannelMessages<T>["DispatcherMessages"]
>;

export class IncomerChannelHandler<
  T extends GenericEvent = GenericEvent
> {
  #subscriber: Types.DatabaseConnection<RedisAdapter>;

  public channels: Map<string, ChannelMessages<T>> = new Map();

  constructor(
    options: IncomerChannelHandlerOptions<T>
  ) {
    Object.assign(this, options);
  }

  set(
    options: SetChannelOptions
  ) {
    const { uuid } = options;

    const channel = new Channel({
      name: uuid
    });

    this.channels.set(uuid, channel);

    return channel;
  }

  get(
    uuid: string
  ): ChannelMessages<T> | null {
    return this.channels.get(uuid) ?? null;
  }

  async remove(
    uuid: string
  ): Promise<void> {
    if (!this.channels.has(uuid)) {
      return;
    }

    await this.#subscriber?.unsubscribe(uuid);
    this.channels.delete(uuid);
  }
}
