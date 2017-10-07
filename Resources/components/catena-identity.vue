<template>
	<div v-if="identity !== null">
	<h1><i class="fa fa-user"></i> {{identity.publicHash}}</h1>
	<table>
		<tr>
			<th colspan="2">
				Details
			</th>
		</tr>

		<tr v-if="identity !== null">
			<td>Public key</td>
			<td><code>{{identity.publicBase58}}</code></td>
		</tr>

		<tr v-if="identity !== null">
			<td>Public hash (hex)</td>
			<td><code>X'{{identity.publicHashHex}}'</code></td>
		</tr>

		<tr v-if="identity !== null">
			<td>Counter</td>
			<td>{{counter}} <a href="javascript:void(0);" @click="updateCounter">Refresh</a></td>
		</tr>

		<tr v-if="identity !== null">
			<td>Private key</td>
			<td>
				<code v-if="privateKeyVisible">{{identity.privateBase58}}</code>
				<template v-else><i class="fa fa-key" v-if="identity !== null"></i></template>
				<button @click="togglePrivateKey">
					<template v-if="privateKeyVisible">Hide</template>
					<template v-else>Show</template>
				</button>
			</td>
		</tr>
	</table>

	<h2>Grants</h2>
	<catena-query v-if="agent !== null" :sql="grantsSQL" :agent="agent"></catena-query>

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
			counter: null
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