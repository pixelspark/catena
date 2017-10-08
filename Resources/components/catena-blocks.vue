<template>
	<div>
		<aside>
			<template v-if="index != null">
				<catena-chain 
					:hash="index.highest" 
					@select="select" 
					:connection="agent.connection"
					:selected-hash="selectedBlock ? selectedBlock.hash: null"/>
			</template>
		</aside>

		<article>
			<template v-if="selectedBlock !== null">
				<catena-block-details 
					:block="selectedBlock"
					@select="selectHash"
				>
				</catena-block-details>
			</template>
		</article>
	</div>
</template>

<script>
const Agent = require("./blockchain").Agent;

module.exports = {
	props: {
		agent: Agent,
		index: Object
	},

	data: function() {
		return {selectedBlock: null};
	},

	methods: {
		selectHash: function(hash) {
			var self = this;
			self.selectedBlock = null;

			self.agent.connection.fetch(hash, function(b) {
				self.selectedBlock = b;
			});
		},

		select: function(block) {
			this.selectedBlock = block;
		},

		goToBlock: function(evt) {
			this.selectHash(evt.target.value);
		}
	}
};
</script>