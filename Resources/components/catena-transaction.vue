<template>
	<div class="catena-transaction">
		<dl>
			<dt>Query</dt>
			<dd><code>{{sql}}</code></dd>

			<dt>Database</dt>
			<dd><code>{{database}}</code></dd>
		
			<dt>Invoker</dt>
			<dd>
				<select @change="updateInvoker" :value="invoker" :disabled="submitting || submitted">
					<option :value="null">Select...</option>
					<option v-for="(identity, idx) in agent.identities" :value="idx" :key="identity.publicHash">{{identity.publicHash}}</option>
				</select>
			</dd>
		
		<template v-if="counter !== null">
			<dt>Counter</dt>
			<dd>
				{{counter}}
			</dd>
		</template>

		<template v-if="counter !== null && !submitted">
			<dt></dt>
			<dd>
				<button @click="submit" :disabled="submitting || submitted"><i class="fa fa-check"></i> Sign and submit</button>
				<button @click="sign" :disabled="submitting || submitted || transaction !== null"><i class="fa fa-check"></i> Sign</button>
			</dd>
		</template>

		<template v-if="submitted">
			<dt></dt>
			<dd>
				<strong>Submitted!</strong>
				<button @click="reset"><i class="fa fa-restart"></i> Restart</button>
			</dd>
		</template>

		<template v-if="transaction !== null">
			<dt></dt>
			<dd>
				<textarea readonly class="catena-code" v-model="transactionJSONString"></textarea>
			</dd>
		</template>
	</div>
</template>

<script>
const Agent = require('./blockchain').Agent;
const Transaction = require('./blockchain').Transaction;

module.exports = {
	props: {
		sql: String,
		database: String,
		agent: Agent
	},

	data: function() {
		return {invoker: null, counter: null, submitting: false, submitted: false, transaction: null };
	},

	watch: {
		sql: function(nv) {
			this.reset();
		}
	},

	computed: {
		transactionJSONString: function() {
			return this.transaction == null ? "" : JSON.stringify(this.transaction.jsonObject);
		},
	},

	methods: {
		sign: function() {
			let id = this.agent.identities[this.invoker];
			this.transaction = new Transaction(id, this.database, this.counter, this.sql);
			this.transaction.sign();
			if(!this.transaction.verify()) {
				throw new Error("my own transaction should verify");
			}
		},

		submit: function() {
			this.submitting = true;
			this.sign();

			var gossip = {
				t: "tx",
				tx: this.transaction.jsonObject
			};

			this.agent.connection.request(gossip, function(res) {
			});

			this.submitted = true;
		},

		reset: function() {
			this.submitted = false;
			this.submitting = false;
			this.transaction = null;
			this.refresh();
		},

		refresh: function() {
			var self = this;

			if(this.invoker !== null) {
				let id = this.agent.identities[this.invoker];
				this.agent.counter(id.publicBase58, function(err, ctr) {
					if(ctr === null || typeof(ctr) == 'undefined') {
						self.counter = 0;
					}
					else {
						self.counter = ctr + 1;
					}
				});
			}
		},

		updateInvoker: function(e) {
			var self = this;
			this.invoker = e.target.value;
			this.counter = null;
			this.refresh();
		}
	}
};
</script>