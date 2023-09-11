// Import Node.js Dependencies
import { randomUUID } from "node:crypto";
import timers from "node:timers/promises";

// Import Third-party Dependencies
import {
  initRedis,
  closeAllRedis
} from "@myunisoft/redis";
import * as Logger from "pino";

// Import Internal Dependencies
import { Dispatcher, Incomer } from "../../../../src/index";

// Internal Dependencies Mocks
const mockedIncomerHandleDispatcherMessage = jest.spyOn(Incomer.prototype as any, "handleDispatcherMessages");

const incomerLogger = Logger.pino({
  level: "debug"
});

describe("Init Incomer without Dispatcher alive", () => {
  const eventComeBackHandler = () => void 0;

  let incomer: Incomer;
  let dispatcherIncomer: Incomer;
  let dispatcher: Dispatcher;

  beforeAll(async() => {
    await initRedis({
      port: process.env.REDIS_PORT,
      host: process.env.REDIS_HOST
    } as any);

    await initRedis({
      port: process.env.REDIS_PORT,
      host: process.env.REDIS_HOST
    } as any, "subscriber");

    incomer = new Incomer({
      name: "foo",
      logger: incomerLogger,
      eventsCast: [],
      eventsSubscribe: [],
      eventCallback: eventComeBackHandler,
      dispatcherInactivityOptions: {
        publishInterval: 2_000,
        maxPingInterval: 3_000
      },
      externalsInitialized: true
    });

    dispatcherIncomer = new Incomer({
      name: "bar",
      logger: incomerLogger,
      eventsCast: [],
      eventsSubscribe: [],
      eventCallback: eventComeBackHandler,
      dispatcherInactivityOptions: {
        publishInterval: 2_000,
        maxPingInterval: 3_000
      },
      isDispatcherInstance: true,
      externalsInitialized: true
    });

    dispatcher = new Dispatcher({
      pingInterval: 3_000
    });
  });

  test("Incomer should init without a Dispatcher alive", async() => {
    await incomer.initialize();
    await dispatcherIncomer.initialize();

    await timers.setTimeout(3_000);

    expect(incomer.dispatcherIsAlive).toBe(false);
    expect(dispatcherIncomer.dispatcherIsAlive).toBe(false);
  });

  test("It should register when a Dispatcher is alive", async() => {
    await dispatcher.initialize();

    await timers.setTimeout(3_500);

    expect(incomer.dispatcherIsAlive).toBe(true);
    expect(dispatcherIncomer.dispatcherIsAlive).toBe(true);
    expect(mockedIncomerHandleDispatcherMessage).toHaveBeenCalled();
  })

  test(`It should set the dispatcher state at false when there is not Dispatcher sending ping`, async() =>
  {
    dispatcher.close();

    await timers.setTimeout(3_500);

    expect(dispatcherIncomer.dispatcherIsAlive).toBe(false);
    expect(incomer.dispatcherIsAlive).toBe(false);
  });

  test("It should set the dispatcher state at true when there is a Dispatcher sending ping", async() => {
    const idleTime = 2_000;

    await timers.setTimeout(idleTime);

    const secondDispatcher = new Dispatcher({
      idleTime: idleTime,
      pingInterval: 3_000
    });
    await secondDispatcher.initialize()

    await timers.setTimeout(5_000);

    expect(dispatcherIncomer.dispatcherIsAlive).toBe(true);
    expect(incomer.dispatcherIsAlive).toBe(true);

    secondDispatcher.close();
  });

  afterAll(async() => {
    setImmediate(async() => {
      await dispatcherIncomer.close();
      await incomer.close();
      await closeAllRedis();
    });
  });
});
