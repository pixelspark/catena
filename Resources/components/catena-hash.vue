<template>
	<div :style="{'position': 'relative', 'display': 'inline-block', 'min-width': (colors.length * 4)+'px', 'padding-bottom': '6px'}" :class="{'catena-hash':true, 'expanded': expanded}">
		<code v-if="expanded">{{hash}}</code>
		<abbr :title="hash" v-if="!expanded" @click="expand(true)">{{shortHash}}</abbr><br/>
		<div style="position: absolute; bottom: 4px; height: 5px; width: 100%; overflow: hidden; vertical-align:top;" class="catena-hash-fingerprint">
			<div v-for="(color,idx) in colors" :col="color" :style="{'background-color': color, 'display': 'block', 'width': Math.round(100/colors.length,0)+'%', 'height': '5px', 'margin': '0px', 'float': 'left'}" :key="idx"></div>
		</div>
	</div>
</template>

<script>
module.exports = {
	props: {
		hash: { type: String },
		format: {type: String, default: "hex"}, /* or base64 */
		expandable: { type: Boolean,  default: true }
	},

	data: function() {
		return {expanded: false};
	},

	methods: {
		expand: function(e) {
			if(this.expandable) {
				this.expanded = e;
			}
		}
	},

	computed: {
		hex: function() {
			if(this.format == 'hex') {
				return this.hash;
			}
			else if(this.format == 'base64') {
				return window.atob(this.hash).split('').map(function (aChar) {
        			return ('0' + aChar.charCodeAt(0).toString(16)).slice(-2);
      			}).join('').toLowerCase();
			}
		},

		colors: function() {
			var cols = [];
			for(var a=0; a<=this.hex.length; a+=6) {
				let str = this.hex.substr(a, 6);
				while(str.length < 6) str += "0";
				cols.push("#"+str);
			}
			return cols;
		},

		shortHash: function() {
			return this.hash.substr(0,5) + "…"+this.hash.substr(-5);
		}
	}
};
</script>