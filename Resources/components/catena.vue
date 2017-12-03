<template>
	<div>
		<nav>
			<ul>
				<li>
					<img src="static/logo.png" class="logo" alt="Catena"/>
				</li>

				<li v-if="connection == null">
					Address: <input :value="url" placholder="address:port" v-on:keyup="set('url', $event.target.value)">
					<button @click="connect">Connect</button>
				</li> 
				<li v-else>
					{{url}}
						<button @click="disconnect">Disconnect</button>
				</li>
				
				<li v-if="index !== null">
					<span>{{index.peers.length}} peers</span>,
					<span>{{index.height}} blocks</span>
					<span>(last updated at <catena-timestamp :timestamp="index.time"></catena-timestamp>)</span>
				</li>
			</ul>
		</nav>

		<main v-if="error === null && connection !== null && (index === null || agent === null)">
			<center>
				<catena-spinner width="64" height="64"></catena-spinner>
			</center>
		</main>

		<main v-if="error !== null">
			<center><i class="fa fa-exclamation-triangle"></i> {{error}}</center>
		</main>

		<main v-if="agent !== null && index !== null && error === null" >
			<catena-tabs>
				<catena-tab name="Welcome">
					<article>
						<h1>Welcome!</h1>
						<p>To get started with Catena, take the following steps:</p>
						<h2>
							<i class="fa fa-check" style="color: rgb(153,204,0);" v-if="agent.identities.length &gt; 0"></i> 
							<i class="fa fa-arrow-right" v-else></i> 
							1. Create an identity
						</h2>
						<p>In order to submit transactions to a Catena database, you need an identity. This identity consists of a special 'key' that allows you to sign transactions.</p>

						<h2>
							<i class="fa fa-check" style="color: rgb(153,204,0);" v-if="ownedDatabases.length &gt; 0"></i> 
							<i class="fa fa-arrow-right" v-else></i>
							2. Create a database
						</h2>
						<p>When you create a database, you become the owner of the database. Only your key can sign transactions that make changes to this database, unless you grant privileges to other keys as well.</p>
						<p v-if="ownedDatabases.length &gt; 0">You currently own database(s) {{ownedDatabases.join(', ')}}.</p>
						<catena-expander title="Create a database" icon="plus">
							<dl>
								<dt>Name of the new database:</dt>
								<dd><input v-model="newDatabaseName" type="text"></dd>
							</dl>
							<catena-transaction :sql="newDatabaseSQL" :database="newDatabaseName" :agent="agent"></catena-transaction>
						</catena-expander>

						<h2>3. Create tables and add some data</h2>
						<p>As database owner, you can perform any operation on your freshly created database on the 'Data' tab.</p>
						
						<h2>4. Grant other users rights on your database</h2>
						<p>Generate other identities on the 'Identities' tab and grant them rights to allow others to perform operations on your database.</p>
						
					</article>					
				</catena-tab>

				<catena-tab name="Data">
					<catena-data :agent="agent" :head="index.highest"></catena-data>
				</catena-tab>

				<catena-tab name="Identities">
					<catena-identities :agent="agent" :head="index.highest"></catena-identities>
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
			error: null,
			newDatabaseName: "foo",
			ownedDatabases: []
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
		},

		newDatabaseSQL: function() {
			return "CREATE DATABASE \""+this.newDatabaseName+"\";";
		}
	},

	methods: {
		connect: function() {
			try {
				var self = this;
				self.error = null;

				var url = this.wsScheme + "://" + this.url + "?uuid=" + this.uuid;
				this.connection = new Connection(url, function(err) {
					if(err!==null) {
						self.error = "Unable to connect!";
					}
					else {
						self.update();
					}
				});

				this.agent = new Agent(this.scheme + "://" + this.url, this.connection);

				this.connection.onReceiveBlock = function(x) {
					self.onReceiveBlock(x);
				};

				this.agent.databasesIOwn(function(err, owned) {
					self.ownedDatabases = owned;
				});
			}
			catch(e) {
				self.error = e.getMessage();
			}	
		},

		disconnect: function() {
			this.connection.disconnect();
			this.connection = null;
			this.index = null;
			this.selectedBlock = null;
			this.error = null;
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