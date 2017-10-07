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
	</div>
</template>

<script>
const Identity = require("./blockchain").Identity;
const Agent = require("./blockchain").Agent;

module.exports = {
	props: {
		identity: Identity,
		agent: Agent
	},

	data: function() {
		return { privateKeyVisible: false };
	},

	computed: {
		grantsSQL: function() {
			return "SELECT * FROM grants WHERE user=X'"+this.identity.publicHashHex+"';";
		}
	},

	methods: {
		togglePrivateKey: function() {
			this.privateKeyVisible = !this.privateKeyVisible;
		}
	}
};
</script>