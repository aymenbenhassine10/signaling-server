// requires
const { log } = require("console");
const express = require("express");
const app = express();
var http = require("http").Server(app);
var kurento = require("kurento-client");
var minimist = require("minimist");

var socketServer = require("http").createServer(app);
var io = require("socket.io")(socketServer, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
});

/* Listen for socket connection on port 3000 */
socketServer.listen(3000, function () {
  console.log("Socket server listening on : 3000");
});

io.on("disconnect", function (reason) {
  console.log("User 1 disconnected because " + reason);
});

// variables
var kurentoClient = null;
var iceCandidateQueues = {};

// constants
var argv = minimist(process.argv.slice(2), {
  default: {
    as_uri: process.env.AS_URI || "http://localhost:3000/",
    ws_uri: process.env.WS_URI || "ws://localhost:8888/kurento",
  },
});

// signaling
io.on("connection", function (socket) {
  console.log("a user connected " + socket.id);
  socket.on("disconnect", function (reason) {
    leaveRoomOnDisconnect(socket, (err) => {
      if (err) {
        console.log(err);
      }
    });
  });

  socket.on("message", function (message) {
    console.log("Message received: ", message.event);

    switch (message.event) {
      case "joinRoom":
        console.log("case joinRoom");

        joinRoom(socket, message.userName, message.roomName, (err) => {
          if (err) {
            console.log(err);
          }
        });
        break;

      case "receiveVideoFrom":
        console.log("case receiveVideoFrom");
        receiveVideoFrom(
          socket,
          message.userid,
          message.roomName,
          message.sdpOffer,
          (err) => {
            if (err) {
              console.log(err);
            }
          }
        );
        break;

      case "candidate":
        console.log("case candidate");

        addIceCandidate(
          socket,
          message.userid,
          message.roomName,
          message.candidate,
          (err) => {
            if (err) {
              console.log(err);
            }
          }
        );
        break;

      //left participant
      case "participantLeft":
        console.log("case leaveRoom");

        leaveRoom(socket, message.roomName, (err) => {
          if (err) {
            console.log(err);
          }
        });
        break;
    }
  });
});

// get roomName by Socket ID
function getRoomNameBySocketId(socketId) {
  let result = "";
  let list = Array.from(io.sockets.adapter.rooms);
  list = list
    .filter(([key, value]) => Object(value).participants)
    .filter(([key, value]) => {
      if (Object(value.participants[socketId]).id == socketId) result = key;
    });
  return result;
}

function leaveRoomOnDisconnect(socket, callback) {
  let roomname = getRoomNameBySocketId(socket.id);
  if (roomname) {
    leaveRoom(socket, roomname, callback);
  }
}

//left Room
function leaveRoom(socket, roomname, callback) {
  console.log("leaving room");
  socket.leave(roomname);
  let myRoom = io.sockets.adapter.rooms.get(roomname);
  if (myRoom) {
    let user = myRoom.participants[socket.id];
    console.log("user left : ", user);

    if (user) {
      delete myRoom.participants[socket.id];
      // Notify other participants that a user has left the room
      socket.to(roomname).emit("message", {
        event: "leaveRoom",
        userid: socket.id,
      });
    }
  }
  // Send callback with success
  callback(null);

  // console.log(io.sockets.adapter.rooms.values());

  // console.log(io.sockets.adapter.rooms.values().get(socket.id))
  // io.sockets.adapter.rooms.map((key,value)=>{console.log("key : ",key)})
  // console.log("socket id : ",socket.id,"rooms  : ",io.sockets.adapter.rooms,"room of this socket : ",io.sockets.adapter.rooms.get(socket.id));
}

// signaling functions
function joinRoom(socket, username, roomname, callback) {
  getRoom(socket, roomname, (err, myRoom) => {
    console.log("inside getRoom callback" + myRoom);
    if (err) {
      return callback(err);
    }

    myRoom.pipeline.create("WebRtcEndpoint", (err, outgoingMedia) => {
      if (err) {
        return callback(err);
      }

      var user = {
        id: socket.id,
        name: username,
        outgoingMedia: outgoingMedia,
        incomingMedia: {},
      };

      let iceCandidateQueue = iceCandidateQueues[user.id];
      if (iceCandidateQueue) {
        while (iceCandidateQueue.length) {
          let ice = iceCandidateQueue.shift();
          console.error(
            `user: ${user.name} collect candidate for outgoing media`
          );
          user.outgoingMedia.addIceCandidate(ice.candidate);
        }
      }

      user.outgoingMedia.on("OnIceCandidate", (event) => {
        let candidate = kurento.register.complexTypes.IceCandidate(
          event.candidate
        );
        socket.emit("message", {
          event: "candidate",
          userid: user.id,
          candidate: candidate,
        });
      });

      socket.to(roomname).emit("message", {
        event: "newParticipantArrived",
        userid: user.id,
        username: user.name,
      });

      let existingUsers = [];
      for (let i in myRoom.participants) {
        if (myRoom.participants[i].id != user.id) {
          existingUsers.push({
            id: myRoom.participants[i].id,
            name: myRoom.participants[i].name,
          });
        }
      }
      socket.emit("message", {
        event: "existingParticipants",
        existingUsers: existingUsers,
        userid: user.id,
      });

      myRoom.participants[user.id] = user;
    });
  });
}

function receiveVideoFrom(socket, userid, roomname, sdpOffer, callback) {
  getEndpointForUser(socket, roomname, userid, (err, endpoint) => {
    if (err) {
      return callback(err);
    }

    endpoint.processOffer(sdpOffer, (err, sdpAnswer) => {
      if (err) {
        return callback(err);
      }

      socket.emit("message", {
        event: "receiveVideoAnswer",
        senderid: userid,
        sdpAnswer: sdpAnswer,
      });

      endpoint.gatherCandidates((err) => {
        if (err) {
          return callback(err);
        }
      });
    });
  });
}

function addIceCandidate(socket, senderid, roomname, iceCandidate, callback) {
  let user = io.sockets.adapter.rooms.get(roomname).participants[socket.id];
  if (user != null) {
    let candidate = kurento.register.complexTypes.IceCandidate(iceCandidate);
    if (senderid == user.id) {
      if (user.outgoingMedia) {
        user.outgoingMedia.addIceCandidate(candidate);
      } else {
        iceCandidateQueues[user.id].push({ candidate: candidate });
      }
    } else {
      if (user.incomingMedia[senderid]) {
        user.incomingMedia[senderid].addIceCandidate(candidate);
      } else {
        if (!iceCandidateQueues[senderid]) {
          iceCandidateQueues[senderid] = [];
        }
        iceCandidateQueues[senderid].push({ candidate: candidate });
      }
    }
    callback(null);
  } else {
    callback(new Error("addIceCandidate failed"));
  }
}

// useful functions
async function getRoom(socket, roomname, callback) {
  console.log("getting room");
  var myRoom = io.sockets.adapter.rooms.get(roomname) || { length: 0 };
  var numClients = myRoom.length;

  console.log(roomname, " has ", numClients, " clients");

  if (numClients == 0) {
    await socket.join(roomname);
    myRoom = io.sockets.adapter.rooms.get(roomname);
    getKurentoClient((error, kurento) => {
      kurento.create("MediaPipeline", (err, pipeline) => {
        if (error) {
          return callback(err);
        }

        myRoom.pipeline = pipeline;
        myRoom.participants = {};
        callback(null, myRoom);
      });
    });
  } else {
    await socket.join(roomname);
    callback(null, myRoom);
  }
  socket.on("info", (message) => {
    console.log("Sent info: ");
    socket.emit("info", "You are in room baby!");
  });
}
function getEndpointForUser(socket, roomname, senderid, callback) {
  console.log("getting endpoint for user");
  var myRoom = io.sockets.adapter.rooms.get(roomname);
  var asker = myRoom.participants[socket.id];
  var sender = myRoom.participants[senderid];

  if (asker.id === sender.id) {
    return callback(null, asker.outgoingMedia);
  }

  if (asker.incomingMedia[sender.id]) {
    sender.outgoingMedia.connect(asker.incomingMedia[sender.id], (err) => {
      if (err) {
        return callback(err);
      }
      callback(null, asker.incomingMedia[sender.id]);
    });
  } else {
    myRoom.pipeline.create("WebRtcEndpoint", (err, incoming) => {
      if (err) {
        return callback(err);
      }

      asker.incomingMedia[sender.id] = incoming;

      let iceCandidateQueue = iceCandidateQueues[sender.id];
      if (iceCandidateQueue) {
        while (iceCandidateQueue.length) {
          let ice = iceCandidateQueue.shift();
          console.error(
            `user: ${sender.name} collect candidate for outgoing media`
          );
          incoming.addIceCandidate(ice.candidate);
        }
      }

      incoming.on("OnIceCandidate", (event) => {
        let candidate = kurento.register.complexTypes.IceCandidate(
          event.candidate
        );
        socket.emit("message", {
          event: "candidate",
          userid: sender.id,
          candidate: candidate,
        });
      });

      sender.outgoingMedia.connect(incoming, (err) => {
        if (err) {
          return callback(err);
        }
        callback(null, incoming);
      });
    });
  }
}

function getKurentoClient(callback) {
  if (kurentoClient !== null) {
    return callback(null, kurentoClient);
  }

  kurento(argv.ws_uri, function (error, _kurentoClient) {
    if (error) {
      console.log("Could not find media server at address " + argv.ws_uri);
      return callback(
        "Could not find media server at address" +
          argv.ws_uri +
          ". Exiting with error " +
          error
      );
    }

    kurentoClient = _kurentoClient;
    callback(null, kurentoClient);
  });
}
