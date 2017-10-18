var app = require('express')();
var http = require('http').Server(app);
var io = require('socket.io')(http);
var kafkaLocal = require('./kafka');

var socketIdToRoomNameMap = {};
var socketToConsumerMap = {};
var port = 3000;
var allConsumersEver = [];

io.on('connection', function (socket) {
    socket.on('register', function (id) {
        console.log('Socket registration for: ', id);
        socketIdToRoomNameMap[socket.id] = id;
        kafkaLocal.getConsumer(id, function(consumer) {
            socketToConsumerMap[id] = consumer;
            if (socketToConsumerMap[id]) {
                socketToConsumerMap[id].on('message',  (message) => {
                    console.log(`Consumer for Socket ${id} received message:`);
                    try {
                        var newMessage = JSON.parse(message.value);
                        console.log(newMessage);
                        // emit messages to client
                        socket.emit('message', newMessage);
                    } catch (error) {
                        console.log(error);
                    }
                });
                console.log(`Connected to consumer for Socket ${id}`);    
                if (allConsumersEver.indexOf(id) === -1) {
                    allConsumersEver.push(id);
                }            
            }
        });
        
    });

    socket.on('message', function (message) {
        console.log(`message from ${socketIdToRoomNameMap[socket.id]}`, message);
        var allTopics = [];
        allTopics = allConsumersEver.filter(con => con!== socketIdToRoomNameMap[socket.id]);
        if (allTopics.length) {
            kafkaLocal.produce(allTopics, {
                message
            });
        }
    });

    socket.on('disconnect', function () {
        var deleting = socketIdToRoomNameMap[socket.id];
        kafkaLocal.closeConsumer(socketToConsumerMap[deleting], function(){
            console.log(`Consumer disconnected for Socket #${deleting}`);
        });
        delete socketIdToRoomNameMap[socket.id];        
        delete socketToConsumerMap[deleting];
    });
});

http.listen(port, function(){
  console.log('listening on port:', port);
});