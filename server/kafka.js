var properties = require('../properties');
var kafka = require('kafka-node'),
    Producer = kafka.Producer,
    kafkaClient = new kafka.KafkaClient({
        kafkaHost: properties.kafkaHost
    }),
    producer = new Producer(kafkaClient);

console.log('Connected to Kafka, possibly!');
let test_message = {
    stuff: 'We are connected!'
};

producer.send([{
    topic: 'amrutha-test2',
    key: 'test-key',
    messages: [JSON.stringify(test_message)]
    }], function (err, data) {
        console.log(err, data);
    });

producer.on('error', function (err) {});
var checkAndAddTopic = function(topic, cb) {
    var client = new kafka.KafkaClient({
        kafkaHost: properties.kafkaHost
    });
    client.once('connect', function () {
        client.loadMetadataForTopics([], function (error, results) {
        if (error) {
            return console.error(error);
        }
        if (results && results.length &&
            results.length >= 2 &&
            results[1] && results[1].metadata &&
            Object.keys(results[1].metadata) &&
            Object.keys(results[1].metadata).indexOf(topic) === -1) {
                producer.createTopics([topic], false, function(err, data) {
                    console.log('Created topic - ', topic);
                    cb();                
                });
            } else {
                cb();
            }
        });
    });
};

var KafkaLocal = {
    produce: function(topics, message) {
        var payloads = topics.map( topic => {
            return {
                topic,
                messages: [JSON.stringify(message)]
            };
        });
        producer.send(payloads, (err, data) => {
            if (err) {
                console.log('Error: ', err);
            }
            if (data) {
                console.log('Data: ', data);
            }
        });
    },
    getConsumer: function(id, cb) {
        var newConsumer;
        checkAndAddTopic(id, function() {
            var client = new kafka.KafkaClient({
                kafkaHost: properties.kafkaHost
            });
            newConsumer = new kafka.Consumer(
                client,
                [
                    { 
                        topic: id,
                        offset: 0
                    }
                ],
                {
                    groupId: 'kafka-node-group'          
                }
            );
            cb(newConsumer);                
        });
    },
    closeConsumer: function(con, cb) {
        if(con) {
            con.close(true, cb);
        }
    }
}
module.exports = KafkaLocal;