<template>
	<div>
		<nav>
			<ul>
				<li>
					<img src="static/logo.png" class="logo" :alt="$t('app.name')"/>
				</li>

				<li v-if="connection == null">
					{{$t('address')}}: <input :value="url" placholder="address:port" v-on:keyup="set('url', $event.target.value)">
					<button @click="connect">{{$t('connect')}}</button>
				</li> 
				<li v-else>
					{{url}}
						<button @click="disconnect">{{$t('disconnect')}}</button>
				</li>
				
				<li v-if="index !== null">
					<span>{{$tc('peers', index.peers.length, {count: index.peers.length})}}</span>,
					<span>{{$tc('blocks', index.height, {count: index.height})}}</span>
					<span>({{$t('lastUpdatedAt')}} <catena-timestamp :timestamp="index.time"></catena-timestamp>)</span>
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
			<catena-tabs v-model="route.mainTab">
				<catena-tab :name="$t('tab.welcome')">
					<article style="overflow-y: auto;">
						<h1>{{$t('start.welcome')}}</h1>
						<p>{{$t('start.intro')}}</p>
						<h2>
							<i class="fa fa-check" style="color: rgb(153,204,0);" v-if="agent.identities.length &gt; 0"></i> 
							<i class="fa fa-arrow-right" v-else></i> 
							{{$t('start.identity')}}
						</h2>
						<i18n tag="p" path="start.identityDescription"><a place="url" :href="'#'+$t('tab.identities').toLowerCase()">'{{$t('tab.identities')}}'</a></i18n>

						<h2>
							<i class="fa fa-check" style="color: rgb(153,204,0);" v-if="ownedDatabases.length &gt; 0"></i> 
							<i class="fa fa-arrow-right" v-else></i>
							{{$t('start.database')}}
						</h2>
						<p>{{$t('start.databaseDescription')}}</p>
						<p v-if="ownedDatabases.length &gt; 0">{{$tc('start.owned', ownedDatabases.length, {databases: ownedDatabases.join(', ')})}}</p>
						<catena-expander :title="$t('start.createDatabase')" icon="plus">
							<dl>
								<dt>{{$t('start.newDatabaseName')}}</dt>
								<dd><input v-model="newDatabaseName" type="text"></dd>
							</dl>
							<catena-transaction :sql="newDatabaseSQL" :database="newDatabaseName" :agent="agent"></catena-transaction>
						</catena-expander>

						<h2>{{$t('start.data')}}</h2>
						<i18n path="start.dataDescription" tag="p"><a place="url" :href="'#'+$t('tab.data').toLowerCase()">'{{$t('tab.data')}}'</a></i18n>
						
						<h2>{{$t('start.grants')}}</h2>
						<i18n path="start.grantsDescription" tag="p"><a place="url" :href="'#'+$t('tab.identities').toLowerCase()">'{{$t('tab.identities')}}'</a></i18n>
						
						<catena-expander :title="$t('start.submitRaw')" icon="paper-plane">
							<catena-raw-transaction :agent="agent"></catena-raw-transaction>
						</catena-expander>
					</article>					
				</catena-tab>

				<catena-tab :name="$t('tab.data')">
					<catena-data :agent="agent" :head="index.highest"></catena-data>
				</catena-tab>

				<catena-tab :name="$t('tab.identities')">
					<catena-identities :agent="agent" :head="index.highest"></catena-identities>
				</catena-tab>

				<catena-tab :name="$t('tab.blocks')">
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

class Route {
	constructor() {
		this._mainTab = "";
		var self = this;
		window.onhashchange = function() { self.parse(); }
		this.parse();
	}

	get mainTab() { return this._mainTab; }
	set mainTab(nv) { this._mainTab = nv; this.update(); }

	parse() {
		let parsed = window.location.hash.substr(1).split("/");
		if(parsed.length>0) {
			this._mainTab = parsed[0];
		}
	}

	update() {
		window.location.hash = "#" + [this._mainTab].join("/");
	}
};

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
			route: new Route(),
			url: host + path,
			scheme: scheme,
			uuid: generateUUID(),
			index: null,
			connection: null,
			agent: null,
			transactions: [],
			error: null,
			newDatabaseName: "foo",
			ownedDatabases: [],
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
				self.error = e.toLocaleString();
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
	},

	i18n: { messages: {
		nl: {
			address: "Adres",
			connect: "Verbind",
			disconnect: "Verbreek verbinding",
			peers: "geen peers | 1 peer | {count} peers",
			blocks: "geen blokken | 1 blok | {count} blokken",
			lastUpdatedAt: "laatst bijgewerkt om",

			tab: {
				welcome: "Welkom",
				data: "Data",
				identities: "Identiteiten",
				blocks: "Blokken"
			},

			start: {
				welcome: "Welkom!",
				intro: "Om te beginnen met Catena neem je de volgende stappen:",
				identity: "1. Maak een identiteit",
				database: "2. Maak een database",
				data: "3. Maak tabellen en voeg data toe",
				grants: "4. Geef andere gebruikers rechten op je database",
				submitRaw: "Ruwe transactie inzenden",
				newDatabaseName: "Naam van de nieuwe database:",
				createDatabase: "Maak een nieuwe database",
				dataDescription: "Als eigenaar van een database kun je alle operaties op je vers gemaakte database uitvoeren op de {url}-tab.",
				grantsDescription: "Genereer andere identiteiten op de {url} tab en verleen deze rechten om andere gebruikers toe te staan wijzigingen door te voeren in de database.",
				databaseDescription: "Wanneer je een nieuwe database aanmaakt wordt je daarvan automatisch de eigenaar. Alleen jij kunt met je sleutel transacties ondertekenen die wijzigingen aanbrengen in deze database, tenzij je privileges daarvoor verleent aan andere gebruikers.",
				identityDescription: "Om transacties te kunnen versturen naar een Catena-database heb je een identiteit nodig. Een identiteit bestaat uit een speciale 'sleutel' waarmee je transacties kunt ondertekenen. Je kunt je identiteiten beheren op de {url}-tab.",
				owned: "Je bent op dit moment geen eigenaar van een database. | Je bent op dit moment eigenaar van de database {databases}. | Je bent op dit moment eigenaar van de databases {databases}.",
			}
		},

		en: {
			address: "Address",
			connect: "Connect",
			disconnect: "Disconnect",
			peers: "no peers | 1 peer | {count} peers",
			blocks: "no blocks | 1 block | {count} blocks",
			lastUpdatedAt: "last updated at",

			tab: {
				welcome: "Welcome",
				data: "Data",
				identities: "Identities",
				blocks: "Blocks"
			},

			start: {
				welcome: "Welcome!",
				intro: "To get started with Catena, take the following steps:",
				identity: "1. Create an identity",
				database: "2. Create a database",
				data: "3. Create tables and add some data",
				grants: "4. Grant other users rights on your database",
				submitRaw: "Submit a raw transaction",
				newDatabaseName: "Name of the new database:",
				createDatabase: "Create a database",
				dataDescription: "As database owner, you can perform any operation on your freshly created database on the {url} tab.",
				grantsDescription: "Generate other identities on the {url} tab and grant them rights to allow others to perform operations on your database.",
				databaseDescription: "When you create a database, you become the owner of the database. Only your key can sign transactions that make changes to this database, unless you grant privileges to other keys as well.",
				identityDescription: "In order to submit transactions to a Catena database, you need an identity. This identity consists of a special 'key' that allows you to sign transactions. You can manage identities on the {{url}} tab.",
				owned: "You currently own no databases. | You currently own the database {databases}. | You currently own the databases {databases}.",
			}
		}
	} }
};
</script>