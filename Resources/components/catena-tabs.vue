<template>
	<div :class="{'catena-tabs':true, 'first-tab-selected': currentTab == 0}">
		<ul>
			<li 
				v-for="(tab, index) in tabs"
				@click="select(index)"
				:class="{'selected': currentTab == index}">
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
	template: '#catena-tabs',

	data: function() {
		return {tabs: [], currentTab: 0};
	},
	
	methods: {
		select: function(idx) {
			this.currentTab = idx;
		}
	}
};
</script>