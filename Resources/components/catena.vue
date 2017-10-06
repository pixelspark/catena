<template>
	<div>
		<nav>
			<h1>Catena</h1>

			<template v-if="connection == null">
				Address: <input :value="url" placholder="address:port" v-on:keyup="set('url', $event.target.value)">
				<button @click="connect">Connect</button>
			</template> 
			<template v-else>
				{{url}}
					<button @click="disconnect">Disconnect</button>
			</template>
			
			<template v-if="index !== null">
				<span>{{index.peers.length}} peers</span>,
				<span>{{index.height}} blocks</span>
				<span>(last updated at <catena-timestamp :timestamp="index.time"></catena-timestamp>)</span>
			</template>
		</nav>

		<main>
			<catena-tabs>
				<catena-tab name="Data">
					<catena-data :url="dataURL"></catena-data>
				</catena-tab>

				<catena-tab name="Blocks">
					<aside>
						<template v-if="index != null">
							<catena-chain 
								:hash="index.highest" 
								@select="select" 
								:connection="connection"
								:selected-hash="selectedBlock ? selectedBlock.hash: null"/>
						</template>
					</aside>

					<article>
						<template v-if="connection != null">
							<input 
								:value="selectedBlock ? selectedBlock.hash : ''" 
								placeholder="Go to block hash..." 
								type="text" 
								v-on:keyup.enter="goToBlock" 
								style="width: 80%; clear: both;"/>

							<a href="javascript:void(0);" @click="selectHash(index.genesis)">Genesis</a>
							<br/>
					</template>

						<template v-if="selectedBlock !== null">
							<catena-block-details 
								:block="selectedBlock"
								@select="selectHash"
							>
							</catena-block-details>
						</template>
					</article>
				</catena-tab>

				<catena-tab name="Peers">
					<article>
						<h1>Peers</h1>
						<ul v-if="index !== null">
							<li v-for="peer in index.peers">{{peer}}</li>
						</ul>
					</article>
					
				</catena-tab>
			</catena-tabs>
		</main>
	</div>
</template>

<script>
function generateUUID() {
    var dt = new Date().getTime();
    var uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        var r = (dt + Math.random()*16)%16 | 0;
        dt = Math.floor(dt/16);
        return (c=='x' ? r :(r&0x3|0x8)).toString(16);
    });
    return uuid;
}

class Connection {
	constructor(url, onConnect) {
		var self = this;
		this.counter = 0;
		this.callbacks = {};
		this.onReceiveBlock = null;
		this.blocks = {};

		this.socket = new WebSocket(url, "catena-v1");
		this.socket.onopen = onConnect;
		this.socket.onmessage = function(x) { self.onReceive(x); };
	}

	disconnect() {
		this.socket.close();
	}

	fetch(blockHash, callback) {
		var self = this;

		if(blockHash in this.blocks) {
			return callback(this.blocks[blockHash]);
		}
		else {
			this.request({t:"fetch", hash: blockHash}, function(response) {
				self.blocks[blockHash] = response.block;
				return callback(response.block);
			});
		}
	}
	
	onReceive(r) {
		try {
			var data = JSON.parse(r.data);
			if(!Array.isArray(data)) { console.log("Receive invalid: ", data); return; }
			
			if(data[0] in this.callbacks) {
				// Solicited
				this.callbacks[data[0]](data[1]);
				delete this.callbacks[data[0]];
			}
			else {
				// Unsolicited
				var self = this;
				
				var replyFunction = function(response) {
					self.connection.send(JSON.stringify([data[0], response]));
				};
				
				this.onReceiveUnsolicited(data[1], replyFunction);
			}
			
		}
		catch(e) {
			console.log("PARSE ERROR: ", r.data, e);
		}
	}
	
	onReceiveUnsolicited(d, reply) {
		switch(d.t) {
			case "query":
				// Indicate we are a passive peer
				reply({t: "passive"});
				
		
			case "block":
				if(this.onReceiveBlock) {
					this.blocks[d.block.hash] = d.block;
					this.onReceiveBlock(d.block);	
				}
				break;
				
			case "tx":
				break;

			case "forget":
				console.log("Peer requests to be forgotton. Its UUID does not match!", d);
				
			default:
				console.log("Unknown unsolicited gossip: ", d);
			
		}
	}
	
	request(req, callback) {
		this.counter += 2;
		this.callbacks[this.counter] = callback;
		this.socket.send(JSON.stringify([this.counter, req]));
	}
};

module.exports = {
	data: function() {
		var initial = {
			url: window.location.host,
			uuid: generateUUID(),
			index: null,
			connection: null,
			transactions: [],
			selectedBlock: null
		};

		try {
			if("catena" in window.localStorage) {
				var saved = JSON.parse(window.localStorage.catena);
				for(var k in saved) {
					if(saved.hasOwnProperty(k)) {
						initial[k] = saved[k];
					}
				}
			}
		}
		catch(e) {}
		return initial;
	},

	created: function() {
		this.connect();
	},

	computed: {
		dataURL: function() {
			return "http://" + this.url;
		}
	},

	methods: {
		select: function(block) {
			this.selectedBlock = block;
		},

		goToBlock: function(evt) {
			this.selectHash(evt.target.value);
		},

		connect: function() {
			try {
				var self = this;

				var url = "ws://" + this.url + "?uuid=" + this.uuid;
				this.connection = new Connection(url, function() {
					self.update();
				});

				this.connection.onReceiveBlock = function(x) {
					self.onReceiveBlock(x);
				};
			}
			catch(e) {
				alert("Error connecting: "+e);
			}	
		},

		disconnect: function() {
			this.connection.disconnect();
			this.connection = null;
			this.index = null;
			this.selectedBlock = null;
		},

		update: function() {
			var self = this;

			self.connection.request({t:"query"}, function(response) { 
				if(response.t == "index") {
					self.index = response.index;
				}
			});
		},

		selectHash: function(hash) {
			var self = this;
			self.selectedBlock = null;

			self.connection.fetch(hash, function(b) {
				self.selectedBlock = b;
			});
		},

		onReceiveBlock: function(b) {
			if(this.index != null && b.index > this.index.height) {
				this.update();
			}
		},
		
		set: function(attr, value) {
			var persistedAttributes = ["url"];
			this[attr] = value;
			var data = {};
			var self = this;
			persistedAttributes.forEach(function(k) {
				data[k] = self[k];
			});
			window.localStorage.catena = JSON.stringify(data);
		}
	}
};
</script>