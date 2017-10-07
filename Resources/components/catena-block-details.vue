<template>
	<div>
		<h1><i class="fa fa-cube" aria-hidden="true"></i> Block #{{block.index}}</h1>
		
		<dl>
			<dt>Hash</dt>
			<dd>
					<catena-hash :hash="block.hash"></catena-hash>
			</dd>

			<dt>Timestamp</dt>
			<dd>
					<catena-timestamp :timestamp="block.timestamp"></catena-timestamp>
			</dd>

			<dt>Miner</dt>
			<dd>
					<catena-hash :hash="block.miner"></catena-hash>
			</dd>

			<dt>Nonce</dt>
			<dd>
				<code>{{block.nonce}}</code>
			</dd>

			<dt>Payload</dt>
			<dd>
					<catena-payload :payload="block.payload"></catena-payload>
			</dd>

			<dt>Related</dt>
			<dd>
					<a href="javascript:void(0);" @click="select(block.previous)">Previous block</a>
			</dd>
		</dl>
	</div>
</template>

<script>
module.exports = {
	props: {
		block: {type: Object}
	},

	methods: {
		select: function(hash) {
			this.$emit('select', hash);
		}
	}
};
</script>