<template>
	<div class="catena-chain">
		<transition-group name="list" tag="ul">
			<catena-block 
				v-for="block in blocks" 
				:key="block.hash"
				:block="block"
				@select="select(block.hash)" 
				:selectedHash="selectedHash"
				:connection="connection"
				:class="selectedHash == block.hash ? 'selected' : ''"
			>
			</catena-block>
		</transition-group>
	</div>
</template>

<script>
module.exports = {
	props: {
		hash: String,
		connection: Object,
		selectedHash: String
	},

	data: function() {
		return {
			first: null,
			blocks: []
		};
	},

	watch: {
		hash: function(nv) {
			this.update();
		}
	},

	created: function() {
		this.update();
	},

	methods: {
		update: function() {
			var self = this;
			
			var blocks = [];
			var count = 0;
			var limit = 100;
			var selectedSeen = false;
			function fetch(h) {
				self.connection.fetch(h, function(b) {
					if(self.first === null) {
						self.first = b.index;
					}

					count++;

					blocks.push(b);
					if(b.index > self.first && count < limit) {
						fetch(b.previous);
					}
					else {
						self.blocks = blocks;
					}
				});
			}

			fetch(this.hash);
		},

		select: function(hash) {
			var self = this;

			self.connection.fetch(hash, function(b) {
				self.$emit('select', b);
			});
		}
	}
};
</script>