<template>
	<div class="catena-query">
		<span v-if="error !== null">Error: {{error}}</span>
		<catena-spinner v-else-if="isLoading" fill="rgb(0,55,100)">Loading...</catena-spinner>
		<div v-else>
			<template v-if="isUnbound">
				<p>Set these parameters first!</p>
			</template>

			<dl v-if="hasParameters">
				<h2>Parameterized query</h2>
				<template v-for="(v, k) in parameters">
					<dt :key="k">{{k}}</dt>
					<dd :key="k"><input type="text" :value="v" @keyup="setParameter(k, $event.target.value)"/></dd>
				</template>

				<dt></dt>
				<dd><button @click="update">Submit</button></dd>
			</dl>

			<template v-if="isMutating">
				<h2>Mutating query</h2>
				<p>The query you have submitted is a mutating query. Do you want to submit a transaction to execute the query?</p>
				<catena-expander title="Create transaction..." icon="plus">
					<catena-transaction :sql="formattedSQL" :agent="agent"></catena-transaction>
				</catena-expander>
			</template>

			<table v-if="!isMutating && result !== null">
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
		agent: Agent,
		head: {type: String, default: null}
	},

	data: function() {
		return { 
			result: null, 
			error: null, 
			isLoading: false,
			isMutating: false, 
			isUnbound: false,
			parameters: {},
			formattedSQL: null,
		};
	},

	watch: {
		sql: function(nv) {
			this.parameters = null;
			this.update();
		},

		head: function() {
			this.update(true);
		}
	},

	created: function() {
		this.update();
	},

	computed: {
		hasParameters: function() {
			if(this.parameters === null) {
				return false
			}

			for(var k in this.parameters) {
				return true
			}

			return false
		}
	},

	methods: {
		setParameter: function(k, v) {
			this.parameters[k] = v;
		},

		update: function(silent) {
			var self = this;
			self.error = null;
			self.result = null;
			if(!silent) {
				self.isLoading = true;
			}

			this.agent.query(this.sql, this.parameters, function(code, res) {
				self.isLoading = false;

				if(code == 200) {
					self.isMutating = false;
					self.isUnbound = false;
					self.error = null;
					self.result = res;
				}
				else if(code == 406) {
					self.parameters = res.parameters;

					if("unbound" in res) {
						self.isUnbound = true;
					}
					else {
						// Mutating query
						self.isUnbound = false;
						self.isMutating = true;
						self.formattedSQL = res.sql;
					}
				}
				else if(res && ("message" in res)) {
					self.error = res.message;
				}
				else {
					self.err = res.statusText;
				}
			});
		}
	}
};
</script>