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
import type { RegisteredIncomer } from "../../types/index.js";

export type IncomerStoreOptions = Partial<KVOptions<RegisteredIncomer>> & {
  adapter: Types.DatabaseConnection<RedisAdapter>;
  idleTime: number;
}

export class IncomerStore extends KVPeer<
  RegisteredIncomer,
  null,
  RedisAdapter
> {
  #key: string;
  #idleTime: number;

  constructor(
    options: IncomerStoreOptions
  ) {
    super({
      ...options,
      prefix: undefined,
      type: "object"
    });

    this.#key = `${options.prefix ? `${options.prefix}-` : ""}incomer`;
    this.#idleTime = options.idleTime;
  }

  #buildIncomerKey(
    incomerId: string
  ): string {
    return `${this.#key}-${incomerId}`;
  }

  isActive(
    incomer: RegisteredIncomer,
    now: number = Date.now()
  ) {
    return now < (Number(incomer.lastActivity) + Number(this.#idleTime));
  }

  async getNonActives(): Promise<RegisteredIncomer[]> {
    const incomers = await this.getIncomers();

    return [...incomers].filter((incomer) => !this.isActive(incomer));
  }

  async setIncomer(
    incomer: Omit<RegisteredIncomer, "providedUUID">,
    providedUUID: string = randomUUID()
  ): Promise<string> {
    await this.setValue({
      key: this.#buildIncomerKey(providedUUID),
      value: {
        ...incomer,
        providedUUID
      }
    });

    return providedUUID;
  }

  async* incomerLazyFetch(count = 5000) {
    let cursor = 0;

    do {
      const [lastCursor, incomerKeys] = await this.adapter.scan(
        cursor,
        "MATCH",
        `${this.#key}-*`,
        "COUNT",
        count
      );

      cursor = Number(lastCursor);

      yield incomerKeys;

      continue;
    }
    while (cursor !== 0);
  }

  async getIncomers(): Promise<Set<RegisteredIncomer>> {
    const incomers: Set<RegisteredIncomer> = new Set();

    for await (const incomerKeys of this.incomerLazyFetch()) {
      const foundIncomers = await Promise.all(incomerKeys.map(
        (incomerKey) => this.getValue(incomerKey)
      ));
      foundIncomers
        .filter((incomer) => incomer !== null)
        .forEach((incomer) => incomers.add(incomer));
    }

    return incomers;
  }

  getIncomer(
    incomerId: string
  ): Promise<RegisteredIncomer | null> {
    return this.getValue(
      this.#buildIncomerKey(incomerId)
    );
  }

  async updateIncomer(
    incomer: RegisteredIncomer
  ): Promise<void> {
    await this.setValue({
      key: this.#buildIncomerKey(incomer.providedUUID),
      value: {
        ...incomer,
        lastActivity: Date.now()
      }
    });
  }

  async updateIncomerState(
    incomerId: string
  ): Promise<void> {
    const incomerKey = this.#buildIncomerKey(incomerId);
    const incomer = await this.getValue(incomerKey);

    if (!incomer) {
      throw new Error(`Cannot find the Incomer ${incomerKey}`);
    }

    this.setValue({
      key: incomerKey,
      value: {
        ...incomer,
        lastActivity: Date.now()
      }
    });
  }

  async deleteIncomer(
    incomerId: string
  ): Promise<void> {
    await this.deleteValue(
      this.#buildIncomerKey(incomerId)
    );
  }
}
