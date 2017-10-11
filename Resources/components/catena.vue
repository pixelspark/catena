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

		<main v-if="agent !== null && index !== null" >
			<catena-tabs>
				<catena-tab name="Data">
					<catena-data :agent="agent" :head="index.highest"></catena-data>
				</catena-tab>

				<catena-tab name="Identities">
					<catena-identities :agent="agent"></catena-identities>
				</catena-tab>

				<catena-tab name="Blocks">
					<catena-blocks :agent="agent" :index="index"></catena-blocks>
				</catena-tab>
			</catena-tabs>
		</main>
	</div>
</template>

<script>
const Connection = require("./blockchain").Connection;
const generateUUID = require("./blockchain").generateUUID;
const Agent = require("./blockchain").Agent;

module.exports = {
	data: function() {
		let scheme = window.location.protocol.substr(0,window.location.protocol.length-1);
		
		// Find the path at which the server is listening.
		var path = "/";
		var host = window.location.host;
		if(scheme !== "file") {
			var pathComponents = window.location.pathname.split(/\//);

			if(pathComponents.length>0) {
				// Strip off the web client itself from the path name (if it's there)
				if(pathComponents[pathComponents.length-1].toLowerCase() == "index.html") {
					pathComponents.pop();
				}
				
				// Prevent double slashes
				pathComponents = pathComponents.filter(function(p) { return p != ''; });

				if(pathComponents.length>0) {
					path = "/" + pathComponents.join("/") + "/";
				}
			}
		}

		// When served from file, use localhost as server
		if(scheme == "file") {
			host = "localhost:8338";	
		}

		/* If the web client is served from file or from an unknown location, 
		assume the server can be found over HTTP. */
		if(!scheme || scheme == "file") {
			scheme = "http";
		}

		var initial = {
			url: host + path,
			scheme: scheme,
			uuid: generateUUID(),
			index: null,
			connection: null,
			agent: null,
			transactions: [],
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
		wsScheme: function() {
			return this.scheme == "https" ? "wss" : "ws";
		}
	},

	methods: {
		connect: function() {
			try {
				var self = this;

				var url = this.wsScheme + "://" + this.url + "?uuid=" + this.uuid;
				this.connection = new Connection(url, function() {
					self.update();
				});

				this.agent = new Agent(this.scheme + "://" + this.url, this.connection);

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