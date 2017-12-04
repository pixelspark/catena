<template>
	<div :class="{'catena-tabs':true, 'first-tab-selected': tabs.length &gt; 0 && currentTab.toLowerCase() == tabs[0].name.toLowerCase()}">
		<ul>
			<li 
				v-for="(tab, index) in tabs"
				@click="select(index)"
				:class="{'selected': currentTab.toLowerCase() == tab.name.toLowerCase()}" :key="index">
				{{tab.name}}
			</li>
		</ul>

		<div>
			<slot></slot>
		</div>
	</div>
</template>

<script>
module.exports = {
	data: function() {
		return {tabs: [], currentTab: this.value};
	},

	props: {
		value: {type: String, default: ""}
	},

	watch: {
		value: function(nv) {
			this.currentTab = nv;
		}
	},
	
	methods: {
		select: function(idx) {
			let name = this.tabs[idx].name.toLowerCase();
			this.currentTab = name;
			this.$emit('input',name);
		}
	}
};
</script>