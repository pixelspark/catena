<template>
	<div>
		<table v-if="data.length &gt; 0">
			<tr>
				<th>{{$t('sql')}}</th>
				<th>{{$t('database')}}</th>
				<th>{{$t('invoker')}}</th>
			</tr>
			<tr v-for="(tr,idx) in transactions" :key="idx">
				<td><code>{{tr.sql}}</code></td>
				<td><code>{{tr.database}}</code></td>
				<td><catena-hash :hash="tr.invoker.publicHash" format="base64"></catena-hash> ({{tr.counter}})</td>
			</tr>
		</table>
		{{seed}}
	</div>
</template>

<script>
const Transaction = require("./blockchain").Transaction;

module.exports = {
	props: {
		payload: {type: String}
	},

	i18n: { messages: {
		en: {
			sql: "SQL",
			invoker: "Invoker",
			database: "Database",
		},
		nl: {
			sql: "SQL",
			invoker: "Opdrachtgever",
			database: "Database",
		}
	} },

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

		transactions: function() {
			return this.data.map(x => Transaction.fromJSON(x));
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