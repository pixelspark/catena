<template>
	<div v-if="identity !== null">
	<h1><i class="fa fa-user"></i> Identity {{identity.publicHash}}</h1>
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
				<code v-if="privateKeyVisible">{{identity.privateBase58}}</code>
				<template v-else><i class="fa fa-key" v-if="identity !== null"></i></template>
				<button @click="togglePrivateKey">
					<template v-if="privateKeyVisible">Hide</template>
					<template v-else>Show</template>
				</button>
			</dd>
		</template>
	</dl>

	<h2>Grants</h2>
	<catena-query v-if="agent !== null" :sql="grantsSQL" :agent="agent"></catena-query>

	<button v-if="!granting" @click="grant(true)">Add...</button><br/>
	<button v-if="granting" @click="grant(false)">Cancel</button><br/>
	<catena-granter v-if="granting" :agent="agent" :user="identity"></catena-granter>

	<h2>Storage</h2>
	<p v-if="!isPersisted">Save this identity in this browser's local storage.</p>
	<p v-if="isPersisted">This identity is saved in the local storage of this browser.</p>
	<button @click="persist" v-if="!isPersisted">Persist</button>
	<button @click="forget" v-if="isPersisted">Forget</button>
	</div>
</template>

<script>
const Identity = require("./blockchain").Identity;
const Agent = require("./blockchain").Agent;
const Transaction = require("./blockchain").Transaction;

module.exports = {
	props: {
		identity: Identity,
		agent: Agent
	},

	data: function() {
		return { 
			privateKeyVisible: false, 
			isPersisted: this.identity ? this.identity.isPersisted : false,
			counter: null,
			granting: false
		};
	},

	watch: {
		identity: function(nv) {
			this.updateCounter();
		}
	},

	created: function() {
		this.updateCounter();
	},

	computed: {
		grantsSQL: function() {
			return "SELECT table, kind FROM grants WHERE user=X'"+this.identity.publicHashHex+"' ORDER BY table ASC;";
		}
	},

	methods: {
		updateCounter: function() {
			var self = this;
			this.counter = null;

			this.agent.counter(this.identity.publicBase58, function(err, ctr) {
				self.counter = ctr || 0;
			});
		},

		grant: function(g) {
			this.granting = g;
		},

		persist: function() {
			this.identity.persist(true);
			this.isPersisted = this.identity.isPersisted;
		},

		forget: function() {
			this.identity.persist(false);
			this.isPersisted = this.identity.isPersisted;
		},

		togglePrivateKey: function() {
			this.privateKeyVisible = !this.privateKeyVisible;
		}
	}
};
</script>