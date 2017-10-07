const Vue = require('vue');
Vue.use(require('vue-resource'));

const App = require('./components/catena.vue');
Vue.component('catena-block', require('./components/catena-block.vue'));
Vue.component('catena-block-details', require('./components/catena-block-details.vue'));
Vue.component('catena-chain', require('./components/catena-chain.vue'));
Vue.component('catena-data', require('./components/catena-data.vue'));
Vue.component('catena-hash', require('./components/catena-hash.vue'));
Vue.component('catena-identity', require('./components/catena-identity.vue'));
Vue.component('catena-identities', require('./components/catena-identities.vue'));
Vue.component('catena-payload', require('./components/catena-payload.vue'));
Vue.component('catena-query', require('./components/catena-query.vue'));
Vue.component('catena-tabs', require('./components/catena-tabs.vue'));
Vue.component('catena-tab', require('./components/catena-tab.vue'));
Vue.component('catena-timestamp', require('./components/catena-timestamp.vue'));

var app = new Vue({
	el: '#app',
	render: h => h(App)
});