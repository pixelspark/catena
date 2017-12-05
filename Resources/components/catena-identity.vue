<template>
	<div v-if="identity !== null">
	<h1><i class="fa fa-user"></i> Identity <catena-hash :hash="identity.publicHash" format="base64"></catena-hash></h1>
	<dl>
		<template v-if="identity !== null">
			<dt>Public key</dt>
			<dd><code>{{identity.publicBase58}}</code></dd>
		</template>

		<template v-if="identity !== null">
			<dt>Public hash (hex)</dt>
			<dd><code>X'{{identity.publicHashHex}}'</code></dd>
		</template>

		<template v-if="identity !== null">
			<dt>Counter</dt>
			<dd>{{counter}} <button @click="updateCounter">Refresh</button></dd>
		</template>

		<template v-if="identity !== null">
			<dt>Private key</dt>
			<dd>
				<catena-expander title="Show" icon="key">
					<code>{{identity.privateBase58}}</code>
				</catena-expander>
			</dd>
		</template>
	</dl>

	<h2>Owned databases</h2>
	<catena-query v-if="agent !== null" :sql="ownedSQL" :agent="agent" :head="head" :database="database" no-data="This user does not currently own any databases."></catena-query>

	<h2>Grants</h2>
	<dl>
		<dt>Show grants in database:</dt>
		<dd>
			<select v-model="database">
				<option key="" value="">Select a database...</option>
				<option v-for="db in databases" :key="db" :value="db">{{db}}</option>
			</select>
		</dd>
	</dl>

	<catena-query v-if="agent !== null && database != '' " :sql="grantsSQL" :agent="agent" :head="head" :database="database" no-data="This user was not granted any privileges for this database."></catena-query>
	<catena-expander title="Grant privileges" icon="plus" v-if="database != '' ">
		<catena-granter :agent="agent" :user="identity" :database="database"></catena-granter>
	</catena-expander>

	<template v-if="identity !== null">
		<h2>Messaging</h2>
		<catena-expander title="Sign a message" icon="hand-spock-o">
			<textarea class="catena-code" v-model="messageToSign" @keyup="clearSignature"></textarea>
			<button @click="signMessage">Sign</button> <button @click="clearSignMessage">Clear</button>

			<dl v-if="messageSignature !== null">
				<dt>Signature</dt>
				<dd><input type="text" style="width: 100%;" readonly disabled :value="messageSignature"/></dd>

				<dt>Combined</dt>
				<dd><input type="text" style="width: 100%;" readonly disabled :value="combinedSignedMessage"/></dd>
			</dl>
		</catena-expander>

		<catena-expander title="Verify a message" icon="handshake-o">
			<textarea class="catena-code" v-model="messageToVerify" @keyup="clearVerify" placeholder="Message (may be combined)"></textarea>
			<input type="text" style="width: 100%;" :placeholder="'Public key (leave empty to use '+identity.publicBase58+')' " v-model="verifyPublicKey" @keyup="clearVerify"/>
			<input type="text" style="width: 100%;" placeholder="Signature" v-model="verifySignature" @keyup="clearVerify"/>
			<button @click="verifyMessage">Verify</button> <button @click="clearVerifyMessage">Clear</button>

			<template v-if="messageVerified !== null">
				<p class="info" v-if="messageVerified">
					<i class="fa fa-check"></i> The message verified successfully
				</p>
				<p class="error" v-else>
					<i class="fa fa-times"></i> The message did not verify
				</p>
			</template>
		</catena-expander>
	</template>

	<h2>Storage</h2>
	<p v-if="!isPersisted">Save this identity in this browser's local storage.</p>
	<p v-if="isPersisted">This identity is saved in the local storage of this browser.</p>
	<button @click="persist" v-if="!isPersisted"><i class="fa fa-bookmark-o"></i>Persist</button>
	<button @click="forget" v-if="isPersisted"><i class="fa fa-eraser"></i> Forget</button>
	</div>
</template>

<script>
const base64 = require('base64-js');
const Identity = require("./blockchain").Identity;
const Agent = require("./blockchain").Agent;
const Transaction = require("./blockchain").Transaction;

module.exports = {
	props: {
		identity: Identity,
		agent: Agent,
		head: {type: String, default: null}
	},

	data: function() {
		return { 
			database: "",
			databases: [],
			isPersisted: this.identity ? this.identity.isPersisted : false,
			counter: null,
			messageToSign: "",
			messageSignature: null,
			messageToVerify: null,
			verifySignature: null,
			messageVerified: null,
			verifyPublicKey: null,
		};
	},

	watch: {
		identity: function(nv) {
			this.updateCounter();
		},

		head: function() {
			this.refreshDatabases();
		}
	},

	created: function() {
		this.updateCounter();
		this.refreshDatabases();
	},

	computed: {
		grantsSQL: function() {
			return "SHOW GRANTS FOR X'"+this.identity.publicHashHex+"';";
		},

		ownedSQL: function() {
			return "SHOW DATABASES FOR X'"+this.identity.publicHashHex+"';";
		},

		combinedSignedMessage: function() {
			return JSON.stringify([this.messageToSign, this.messageSignature]);
		}
	},

	methods: {
		refreshDatabases: function() {
			var self = this;

			this.agent.databases(function(err, databases) {
				databases.sort();
				self.databases = databases;
			});
		},

		verifyMessage: function() {
			if(this.verifySignature === null || this.verifySignature.length == 0) {
				try {
					if(this.messageToVerify === null || this.messageToVerify.length == 0) {
						alert("Please enter a message to verify");
					}
					else {
						let m = JSON.parse(this.messageToVerify);
						if(Array.isArray(m) && m.length == 2) {
							this.messageToVerify = m[0];
							this.verifySignature = m[1];
						}
					}
				}
				catch(e) {
					alert("Please enter a signature");
					return;
				}
			}

			try {
				let sig = base64.toByteArray(this.verifySignature);

				if(this.verifyPublicKey === null) {
					this.messageVerified = this.identity.verify(new Buffer(this.messageToVerify), new Buffer(sig));
				}
				else {
					this.messageVerified = Identity.verify(new Buffer(this.messageToVerify), this.verifyPublicKey, new Buffer(sig));
				}
			}
			catch(e) {
				this.messageVerified = false;
				alert(e);
			}
		},

		clearSignMessage: function() {
			this.messageToSign = null;
			this.messageSignature = null;
		},

		clearVerifyMessage: function() {
			this.messageVerified = null;
			this.messageToVerify = null;
			this.verifySignature = null;
			this.verifyPublicKey = null;
		},

		clearVerify: function() {
			this.messageVerified = null;
		},

		clearSignature: function() {
			this.messageSignature = null;
		},

		updateCounter: function() {
			var self = this;
			this.counter = null;

			this.agent.counter(this.identity.publicBase58, function(err, ctr) {
				self.counter = ctr || 0;
			});
		},

		signMessage: function() {
			let buffer = new Buffer(this.messageToSign);
			this.messageSignature = base64.fromByteArray(this.identity.sign(buffer));
		},

		persist: function() {
			this.identity.persist(true);
			this.isPersisted = this.identity.isPersisted;
		},

		forget: function() {
			this.identity.persist(false);
			this.isPersisted = this.identity.isPersisted;
		}
	}
};
</script>