import { version } from "../package.json";
import { createClient, RedisClientType } from "redis";

type Person = {
    firstName: string,
    lastName: string
}

const people: Person[] = [
    { firstName: "Chad", lastName: "Lee" },
    { firstName: "Jordon", lastName: "Cummings" },
    { firstName: "Alec", lastName: "Hays" },
    { firstName: "Rashad", lastName: "Webster" },
    { firstName: "Princess", lastName: "Conley" },
    { firstName: "Kaydence", lastName: "Mcbride" },
    { firstName: "Leo", lastName: "Smith" },
    { firstName: "Audrey", lastName: "Reid" },
    { firstName: "Aidyn", lastName: "Leblanc" },
    { firstName: "Andrea", lastName: "Stewart" },
    { firstName: "Ezequiel", lastName: "Leach" },
    { firstName: "Charles", lastName: "Wilkinson" },
];

(async () => {
    const client = createClient({
        url: "redis://localhost:6379",

    });

    client.on('error', (err) => console.log('Redis Client Error', err));

    await client.connect();
    // await client.set('hi', 'bye');

    readPeople(client as RedisClientType)

    await processPeople(client as RedisClientType);

    console.log("Start " + version)
})();

async function readPeople(client: RedisClientType) {
    const STREAMS_KEY = "users";
    const GROUP_KEY = "node_1";
    const CONSUMER_KEY = "consumer:1";

    await client.xGroupDestroy(STREAMS_KEY, GROUP_KEY);
    await client.xGroupCreate(STREAMS_KEY, GROUP_KEY, "0");
    const readRes = await client.xReadGroup(GROUP_KEY, CONSUMER_KEY, {
        key: STREAMS_KEY,
        id: '>'
    }, {
        BLOCK: 2000,
        COUNT: 2000,
    });

    await client.xGroupDelConsumer(STREAMS_KEY, GROUP_KEY, CONSUMER_KEY);

    console.log(readRes);
}

async function processPeople(client: RedisClientType) {
    for (const person of people) {
        const args: [string, string, any] = ["users", "*", { firstName: person.firstName, lastName: person.lastName }]

        const res = await client.XADD(...args)
        // console.log(res);
    }
}