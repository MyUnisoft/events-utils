// Import Third-party Dependencies
import {
  RedisAdapter
} from "@myunisoft/redis";
import { pino } from "pino";
import { Ok } from "@openally/result";

// Import Internal Dependencies
import { Incomer } from "../../../../src/index.js";

const incomerLogger = pino({
  level: "debug"
});

const redis = new RedisAdapter({
  port: Number(process.env.REDIS_PORT),
  host: process.env.REDIS_HOST
});
const subscriber = new RedisAdapter({
  port: Number(process.env.REDIS_PORT),
  host: process.env.REDIS_HOST
});

beforeAll(async() => {
  await redis.initialize();
    await subscriber.initialize();

    await redis.flushall();
});

afterAll(async() => {
  await redis.close();
  await subscriber.close();
});

describe("Init Incomer without Dispatcher alive & prefix as \"test\"", () => {
  const eventComeBackHandler = jest.fn().mockImplementation(() => Ok({ status: "RESOLVED" }));;

  describe("With externalsInitialized at true", () => {
    const incomer: Incomer = new Incomer({
      redis,
      subscriber,
      name: "foo",
      prefix: "test",
      logger: incomerLogger,
      eventsCast: ["accountingFolder"],
      eventsSubscribe: [],
      eventCallback: eventComeBackHandler,
      dispatcherInactivityOptions: {
        publishInterval: 5000
      },
      externalsInitialized: true
    });

    test("it should init", async() => {
      await incomer.initialize();

      expect(incomer.externals).not.toBeDefined();
    });

    afterAll(async() => {
      await incomer.close();
    });
  });

  describe("Prefix as \"test\"", () => {
    const incomer: Incomer = new Incomer({
      redis,
      subscriber,
      name: "foo",
      prefix: "test",
      logger: incomerLogger,
      eventsCast: ["accountingFolder"],
      eventsSubscribe: [],
      eventCallback: eventComeBackHandler,
      dispatcherInactivityOptions: {
        publishInterval: 5000
      },
      externalsInitialized: false
    });

    test("it should init", async() => {
      await incomer.initialize();

      expect(incomer.externals).toBeDefined();
    });

    afterAll(async() => {
      await incomer.close();
    });
  });
});


