<template>
	<div>
		<table v-if="data.length &gt; 0">
			<tr>
				<th>SQL</th>
				<th>Invoker</th>
			</tr>
			<tr v-for="tr in data" :key="tr.tx.signature">
				<td><code>{{tr.tx.sql}}</code></td>
				<td><catena-hash :hash="tr.tx.invoker" format="base64"></catena-hash> ({{tr.tx.counter}})</td>
			</tr>
		</table>
		{{seed}}
	</div>
</template>

<script>
module.exports = {
	props: {
		payload: {type: String}
	},

	computed: {
		seed: function() {
		try {
			let e = JSON.parse(atob(this.payload));
			return null;
			}
			catch(e) {
				return atob(this.payload);
			}
		},

		data: function() {
			try {
			return JSON.parse(atob(this.payload));
			}
			catch(e) {
				return [];
			}
		}
	}
};
</script>