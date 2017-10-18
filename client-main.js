var url = 'localhost:3000';
var socket = io(url);
function appendMessage(from, message) {
    $('#messages').append(`<li><b>${from}</b>: ${message}</li>`);
}
window.socketClient = {};
window.socketClient.init = function(instance) {
    socket.emit('register', 'amrutha-' + instance);
    $(function () {
        $('form').submit(function() {
            var msg = $('#m').val();
            appendMessage('Me', msg);
            socket.emit('message', msg);
            $('#m').val('');
            return false;
        });
        socket.on('message', function(msg){
            console.log('message from socket', msg);
            appendMessage('Them', msg.message);
            window.scrollTo(0, document.body.scrollHeight);
        });
    });
}