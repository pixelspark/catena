<template>
	<div :class="{'catena-expander':true, 'expanded': expanded}">
		<button v-if="expanded" class="cancel" @click="expand(false)"><i class="fa fa-times"></i></button>
		<button class="handle" @click="expand(true)"><i :class="iconClass" v-if="icon !== null"></i> {{title}}</button>
		<div v-if="expanded">
			<slot/>
		</div>
	</div>
</template>

<script>
module.exports = {
	props: {
		title: String,
		icon: { type: String, default: null }
	},

	data: function() {
		return { expanded: false };
	},

	computed: {
		iconClass: function() {
			return "fa fa-" + this.icon;
		}
	},

	methods: {
		expand: function(e) {
			this.expanded = e;
		}
	}
};
</script>