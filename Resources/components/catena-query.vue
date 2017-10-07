<template>
	<div class="catena-query">
		<span v-if="error !== null">Error: {{error}}</span>
		<span v-else-if="result == null && !isMutating">Loading...</span>
		<div v-else>
			<template v-if="isMutating">
				<h2>Mutating query</h2>
				<p>The query you have submitted is a mutating query. Do you want to submit a transaction to execute the query?</p>
				<catena-transaction :sql="formattedSQL" :agent="agent"></catena-transaction>
			</template>

			<table v-if="!isMutating">
				<thead>
					<tr>
						<th v-for="col in result.columns" :key="col">{{col}}</th>
					</tr>
				</thead>

				<tbody>
					<tr v-if="!result.rows || result.rows.length == 0" ><td>(No data)</td></tr>

					<tr v-for="(row, idx) in result.rows" :key="idx">
						<td v-for="(cell, idx) in row" :key="idx">
							<span v-if="cell === null" class="null">NULL</span>
							<span v-else>
								{{cell}}
							</span>
						</td>
					</tr>
				</tbody>
			</table>
		</div>
	</div>
</template>

<script>
const Agent = require("./blockchain").Agent;

module.exports = {
	props: {
		sql: String,
		agent: Agent
	},

	data: function() {
		return { result: null, error: null, isMutating: false, formattedSQL: null };
	},

	watch: {
		sql: function(nv) {
			this.update();
		}
	},

	created: function() {
		this.update();
	},

	methods: {
		update: function() {
			var self = this;
			self.error = null;
			self.result = null;
			var data = {sql: this.sql};
			this.$http.post(this.agent.url + "/api/query", data).then(function(r) {
				self.isMutating = false;
				self.result = r.body;
			}, function(r) {
				if(r.status == 406) {
					// Mutating query
					self.isMutating = true;
					self.formattedSQL = r.body.sql;
				}
				else if("message" in r.body) {
					self.error = r.body.message;
				}
				else {
					self.err = r.statusText;
				}
			});
		}
	}
};
</script>