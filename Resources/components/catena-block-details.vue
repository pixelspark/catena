<template>
	<div>
		<h1><i class="fa fa-cube" aria-hidden="true"></i> {{$t('title', {index: block.index})}}</h1>
		
		<dl>
			<dt>{{$t('hash')}}</dt>
			<dd>
					<catena-hash :hash="block.hash"></catena-hash>
			</dd>

			<dt>{{$t('timestamp')}}</dt>
			<dd>
					<catena-timestamp :timestamp="block.timestamp"></catena-timestamp>
			</dd>

			<dt>{{$t('miner')}}</dt>
			<dd>
					<catena-hash :hash="block.miner"></catena-hash>
			</dd>

			<dt>{{$t('nonce')}}</dt>
			<dd>
				<code>{{block.nonce}}</code>
			</dd>

			<dt>{{$t('payload')}}</dt>
			<dd>
					<catena-payload :payload="block.payload"></catena-payload>
			</dd>

			<dt>{{$t('related')}}</dt>
			<dd>
					<a href="javascript:void(0);" @click="select(block.previous)">{{$t('previousBlock')}}</a>
			</dd>
		</dl>
	</div>
</template>

<script>
module.exports = {
	props: {
		block: {type: Object}
	},

	i18n: { messages: {
		en: {
			hash: "Hash",
			timestamp: "Timestamp",
			miner: "Miner",
			nonce: "Nonce",
			payload: "Payload",
			related: "Related",
			previousBlock: "Previous block",
			title: "Block #{index}"
		},
		nl: {
			hash: "Hash",
			timestamp: "Tijdstempel",
			miner: "Miner",
			nonce: "Nonce",
			payload: "Inhoud",
			related: "Gerelateerd",
			previousBlock: "Voorgaand blok",
			title: "Blok #{index}"
		}
	} },

	methods: {
		select: function(hash) {
			this.$emit('select', hash);
		}
	}
};
</script>