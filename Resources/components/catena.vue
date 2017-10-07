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
				<catena-tab name="Data" v-if="agent !== null">
					<catena-data :agent="agent"></catena-data>
				</catena-tab>

				<catena-tab name="Identities" v-if="agent !== null">
					<catena-identities :agent="agent"></catena-identities>
				</catena-tab>

				<catena-tab name="Blocks" v-if="connection !== null">
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

				<catena-tab name="Peers" v-if="connection !== null">
					<article>
						<h1>Peers</h1>
						<ul v-if="index !== null">
							<li v-for="(peer, key) in index.peers" :key="key">{{peer}}</li>
						</ul>
					</article>
					
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
		let scheme = window.location.scheme;
		if(!scheme || scheme == "file") {
			scheme = "http";
		}

		var initial = {
			url: window.location.host,
			scheme: scheme,
			uuid: generateUUID(),
			index: null,
			connection: null,
			agent: null,
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
		wsScheme: function() {
			return this.scheme == "https" ? "wss" : "ws";
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