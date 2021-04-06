// const { Kafka } = require('kafkajs');
const v8 = require("v8");
// const avro = require("avro-js");

require("./stream");

// const primeDataType = avro.parse('./PrimeData.avsc');

const isPrimeValueMapper = (value) => {
    let newValue = Object.assign({}, value);
    let hrTimeStart = process.hrtime()
    newValue.timestampProcessStart = hrTimeStart[1] * 100000000 + hrTimeStart[0];
    newValue.isPrime = true;

    for (let divisor = 2; divisor <= Math.sqrt(newValue.getNumber()); divisor++)
        if (newValue.getNumber() % divisor == 0) { newValue.isPrime = false; break; }
    
    let hrTimeEnd = process.hrtime()
    newValue.timestampProcessEnd = hrTimeEnd[1] * 100000000 + hrTimeEnd[0];
    return newValue;
}

exports.handler = async (event, context) => {
    const kafka = new Kafka({
        clientId: "aws-lambda-worker",
        brokers: event.bootstrapServers
    });

    const producer = kafka.producer();
    await producer.connect();

    let topicMessages = [
        // {
        //     topic: "topic1",
        //     messages: [
        //         { key: "", value: "" }
        //     ]
        // },
        // {
        //     topic: "topic2",
        //     messages: [
        //         { key: "", value: "" }
        //     ]
        // }
    ]

    let numbersInput = new Array();
    for (let partitionKey in event.records) {
        console.log('Partition Key: ', partitionKey)
        // Iterate through records
        numbersInput.push(
            ...event.records[partitionKey].map((record) => {
                console.log('Record: ', record);
                // Decode base64
                let data = Buffer.from(record.value, 'base64').toJSON();
                console.log('data: ', data);
                return { key: record.key, value: data };
            })
        );
    }

    let numbersInputStream = new Stream(numbersInput);

    let mappedNumbersStream = numbersInputStream.mapValues(isPrimeValueMapper);

    let branches = mappedNumbersStream.branch(
        ({key, value})=>value.isPrime,
        ({key, value})=>!value.isPrime
    );

    topicMessages.push(branches[0].to("prime-numbers-output"));
    topicMessages.push(branches[0].to("composite-numbers-output"));

    console.log(topicMessages);
    await producer.sendBatch({
        topicMessages
    });

    producer.disconnect();
}