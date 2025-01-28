// Import Third-party Dependencies
import {
  RedisAdapter
} from "@myunisoft/redis";

// Import Internal Dependencies
import {
  type PartialTransaction,
  type Transaction,
  TransactionStore
} from "../../../../src/class/store/transaction.class.js";

let transactionStore: TransactionStore<"dispatcher">;
const redis = new RedisAdapter({
  port: Number(process.env.REDIS_PORT),
  host: process.env.REDIS_HOST
});

beforeAll(async() => {
  await redis.initialize();

  await redis.flushall();
});

afterAll(async() => {
  await redis.close();
});

describe("Transaction options", () => {
  beforeAll(async() => {
    transactionStore = new TransactionStore<"dispatcher">({
      adapter: redis,
      instance: "dispatcher"
    });
  });

  test("Transaction should be defined", () => {
    expect(transactionStore).toBeInstanceOf(TransactionStore);
  });

  describe("deleteTransaction", () => {
    let transaction: Transaction<"dispatcher">;

    beforeAll(async() => {
      const partialTransaction: PartialTransaction<"dispatcher"> = {
        name: "ping",
        data: null,
        redisMetadata: {
          origin: "foo",
          to: "foo",
          mainTransaction: true,
          relatedTransaction: null,
          iteration: 0,
          resolved: false,
          incomerName: "foo"
        }
      };

      transaction = await transactionStore.setTransaction(partialTransaction);
    });


    test("calling deleteTransaction, it should delete the transaction & return null", async() => {
      await transactionStore.deleteTransaction(transaction.redisMetadata.transactionId);

      const result = await transactionStore.getTransactionById(transaction.redisMetadata.transactionId);

      expect(result).toBeNull();
    });
  });

  describe("setTransaction", () => {
    test("calling setTransaction, it should add a new transaction to the transaction tree", async() => {
      const partialTransaction: PartialTransaction<"dispatcher"> = {
        name: "ping",
        data: null,
        redisMetadata: {
          origin: "foo",
          to: "foo",
          mainTransaction: true,
          relatedTransaction: null,
          iteration: 0,
          resolved: false,
          incomerName: "foo"
        },
      };

      const transaction = await transactionStore.setTransaction(partialTransaction);
      await transactionStore.deleteTransaction(transaction.redisMetadata.transactionId);

      expect(transaction).toBeDefined();
    });
  });

  describe("getTransactionById", () => {
    let transaction: Transaction<"dispatcher">;

    beforeAll(async() => {
      const partialTransaction: PartialTransaction<"dispatcher"> = {
        name: "ping",
        data: null,
        redisMetadata: {
          origin: "foo",
          to: "foo",
          mainTransaction: true,
          relatedTransaction: null,
          iteration: 0,
          resolved: false,
          incomerName: "foo"
        },
      };

      transaction = await transactionStore.setTransaction(partialTransaction);
    });

    afterAll(async() => {
      await transactionStore.deleteTransaction(transaction.redisMetadata.transactionId);
    });

    test("calling getTransactionById, it should return the according transaction", async() => {
      const finalTransaction = await transactionStore.getTransactionById(transaction.redisMetadata.transactionId);

      expect(finalTransaction).toBeDefined();
    });
  });

  describe("getTransactions", () => {
    beforeAll(async() => {
      const transaction: PartialTransaction<"dispatcher"> = {
        name: "ping",
        data: null,
        redisMetadata: {
          origin: "foo",
          to: "foo",
          mainTransaction: true,
          relatedTransaction: null,
          iteration: 0,
          resolved: false,
          incomerName: "foo"
        },
      };

      await transactionStore.setTransaction(transaction);
      await transactionStore.setTransaction(transaction);
    });


    test("calling getTransactions, it should return the transaction tree", async() => {
      const transactionTree = await transactionStore.getTransactions();

      expect(transactionTree).toBeDefined();
      expect(transactionTree.size).toBe(2);

      for (const transactionId of Object.keys(transactionTree)) {
        await transactionStore.deleteTransaction(transactionId);
      }
    });
  });
});
