<template>
	<div>
		<h1><i class="fa fa-cube" aria-hidden="true"></i> Block #{{block.index}}</h1>
		
		<dl>
			<dt><i class="fa fa-clock-o" aria-hidden="true"></i> Time</dt>
			<dd>
					<catena-timestamp :timestamp="block.timestamp"></catena-timestamp>
			</dd>

			<dt><i class="fa fa-certificate" aria-hidden="true"></i> Miner</dt>
			<dd>
					<catena-hash :hash="block.miner"></catena-hash>
			</dd>

			<dt><i class="fa fa-certificate" aria-hidden="true"></i> Nonce</dt>
			<dd>
				<code>{{block.nonce}}</code>
			</dd>

			<dt><i class="fa fa-archive" aria-hidden="true"></i> Payload</dt>
			<dd>
					<catena-payload :payload="block.payload"></catena-payload>
			</dd>

			<dt><i class="fa fa-cube" aria-hidden="true"></i> Related</dt>
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