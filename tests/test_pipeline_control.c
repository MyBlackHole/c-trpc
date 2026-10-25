#include "../src/group/pipeline_control_internal.h"
#include "../src/group/pipeline_registry_internal.h"

#include "../src/execution/reactor.h"
#include "tr/status.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static struct tr_conn_handle fake_connection(struct tr_reactor *reactor,
					     uint32_t slot,
					     uint32_t generation)
{
	struct tr_conn_handle handle;

	memset(&handle, 0, sizeof(handle));
	handle.reactor = reactor;
	handle.slot = slot;
	handle.generation = generation;
	return handle;
}

static void test_pipeline_control_session(void)
{
	struct tr_reactor *owner = NULL;
	struct tr_pipeline_registry_config registry_config;
	struct tr_pipeline_registry *registry = NULL;
	struct tr_pipeline_registry_stats registry_stats;
	struct tr_pipeline_control_config control_config;
	struct tr_pipeline_control *control = NULL;
	struct tr_pipeline_control *duplicate = NULL;
	struct tr_pipeline_data_offer offer0;
	struct tr_pipeline_data_offer offer1;
	struct tr_pipeline_data_offer stale;
	struct tr_pipeline_transfer_ready ready0;
	struct tr_pipeline_transfer_ready ready1;
	struct tr_conn_handle control_connection;
	struct tr_conn_handle data0;
	struct tr_conn_handle data1;

	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &owner) == TR_OK);
	assert(tr_reactor_start(owner) == TR_OK);

	memset(&registry_config, 0, sizeof(registry_config));
	registry_config.owner = owner;
	registry_config.owner_shard_id = 2U;
	registry_config.capacity = 2U;
	assert(tr_pipeline_registry_create(&registry_config, &registry) == TR_OK);

	memset(&control_config, 0, sizeof(control_config));
	control_config.registry = registry;
	control_config.pipeline_id = UINT64_C(0x6001);
	control_config.epoch = UINT64_C(41);
	control_config.data_capacity = 2U;
	control_config.stream_affinity_capacity = 4U;

	control_connection = fake_connection(owner, 10U, 1U);
	assert(tr_pipeline_control_create(
		       &control_config, control_connection, &control) == TR_OK);
	assert(control != NULL);

	/* 注册表唯一性隔断同标识的第二个运行时轮次或会话。 */
	{
		struct tr_pipeline_control_config duplicate_config = control_config;

		duplicate_config.epoch++;
		assert(tr_pipeline_control_create(
			       &duplicate_config, control_connection,
			       &duplicate) == TR_ERR_STATE);
		assert(duplicate == NULL);
	}

	memset(&registry_stats, 0, sizeof(registry_stats));
	assert(tr_pipeline_registry_get_stats(registry, &registry_stats) == TR_OK);
	assert(registry_stats.count == 1U);

	/* 预留只是提议。精确 DATA 连接消耗能力凭证之前，
	 * 禁止 TRANSFER_READY。
	 */
	memset(&offer0, 0, sizeof(offer0));
	assert(tr_pipeline_control_reserve_data(control, &offer0) == TR_OK);
	assert(offer0.route.version == TR_PIPELINE_ROUTE_VERSION);
	assert(offer0.route.role == TR_PIPELINE_ROUTE_DATA);
	assert(offer0.route.owner_shard_id == 2U);
	assert(offer0.route.pipeline_id == UINT64_C(0x6001));
	assert(offer0.route.epoch == UINT64_C(41));
	assert(offer0.route.member_index == offer0.data.index);
	assert(offer0.route.member_generation == offer0.data.generation);

	memset(&ready0, 0, sizeof(ready0));
	assert(tr_pipeline_control_prepare_transfer(
		       control, 1001U, &ready0) == TR_AGAIN);

	/* 伪造取消不得消耗真实能力凭证。 */
	stale = offer0;
	stale.data.generation++;
	stale.route.member_generation++;
	assert(tr_pipeline_control_cancel_data(control, &stale) ==
	       TR_ERR_STALE);

	/* offer1 成为 ATTACHED 时保持 offer0 为 RESERVED。
	 * 即使轮询先遇到该槽位，TRANSFER_READY 也必须跳过预留。
	 */
	memset(&offer1, 0, sizeof(offer1));
	assert(tr_pipeline_control_reserve_data(control, &offer1) == TR_OK);
	data1 = fake_connection(owner, 21U, 1U);
	assert(tr_pipeline_registry_attach_data_route(
		       registry, &offer1.route, data1, NULL) == TR_OK);

	/* 精确对端取消在交接不确定性下保持幂等。
	 * 一旦该代次为 ATTACHED，就成功返回且不做操作，
	 * 不得退役存活成员关系。
	 */
	assert(tr_pipeline_control_cancel_data(control, &offer1) == TR_OK);

	memset(&ready0, 0, sizeof(ready0));
	assert(tr_pipeline_control_prepare_transfer(
		       control, 1001U, &ready0) == TR_OK);
	assert(ready0.stream_id == 1001U);
	assert(ready0.data.index == offer1.data.index);
	assert(ready0.data.generation == offer1.data.generation);

	/* 流亲和绑定恰好建立一次。 */
	assert(tr_pipeline_control_prepare_transfer(
		       control, 1001U, &ready1) == TR_ERR_STATE);

	/* 附接先前 RESERVED 的 offer0；下一个流现在可以使用它。 */
	data0 = fake_connection(owner, 20U, 1U);
	assert(tr_pipeline_registry_attach_data_route(
		       registry, &offer0.route, data0, NULL) == TR_OK);

	memset(&ready1, 0, sizeof(ready1));
	assert(tr_pipeline_control_prepare_transfer(
		       control, 1002U, &ready1) == TR_OK);
	assert(ready1.stream_id == 1002U);
	assert(ready1.data.index == offer0.data.index);
	assert(ready1.data.generation == offer0.data.generation);

	/* 已附接 DATA 或流生命周期仍存在时，CONTROL 不能消失。 */
	assert(tr_pipeline_control_close(control, control_connection) ==
	       TR_ERR_STATE);

	assert(tr_pipeline_control_release_transfer(control, 1001U) == TR_OK);
	assert(tr_pipeline_control_release_transfer(control, 1002U) == TR_OK);
	assert(tr_pipeline_control_release_transfer(control, 1002U) ==
	       TR_ERR_STALE);

	assert(tr_pipeline_registry_detach_data_route(
		       registry, &offer0.route, data0) == TR_OK);
	assert(tr_pipeline_registry_detach_data_route(
		       registry, &offer1.route, data1) == TR_OK);

	/* 同一精确代次已为 FREE 时也是幂等的无操作。 */
	assert(tr_pipeline_control_cancel_data(control, &offer1) == TR_OK);
	stale = offer1;
	stale.data.generation++;
	stale.route.member_generation++;
	assert(tr_pipeline_control_cancel_data(control, &stale) ==
	       TR_ERR_STALE);

	/* RESERVED 能力凭证属于 CONTROL。错误 CONTROL 代次的关闭
	 * 必须在提交前失败，不能先取消预留再尝试恢复 CONTROL。
	 */
	assert(tr_pipeline_control_reserve_data(control, &offer0) == TR_OK);
	{
		struct tr_conn_handle stale_control = control_connection;
		struct tr_pipeline_stats before_close;

		stale_control.generation++;
		assert(tr_pipeline_control_close(control, stale_control) ==
		       TR_ERR_STALE);
		memset(&before_close, 0, sizeof(before_close));
		assert(tr_pipeline_control_get_stats(control, &before_close) ==
		       TR_OK);
		assert(before_close.control_bound == 1);
		assert(before_close.data_reserved_count == 1U);
	}
	assert(tr_pipeline_control_close(control, control_connection) == TR_OK);
	control = NULL;

	memset(&registry_stats, 0, sizeof(registry_stats));
	assert(tr_pipeline_registry_get_stats(registry, &registry_stats) == TR_OK);
	assert(registry_stats.count == 0U);

	tr_pipeline_registry_destroy(registry);
	assert(tr_reactor_stop(owner) == TR_OK);
	tr_reactor_destroy(owner);
}

static void test_pipeline_control_registry_capacity_failure_is_precommit(void)
{
	struct tr_reactor *owner = NULL;
	struct tr_pipeline_registry_config registry_config;
	struct tr_pipeline_registry *registry = NULL;
	struct tr_pipeline_registry_stats stats;
	struct tr_pipeline_control_config config;
	struct tr_pipeline_control *first = NULL;
	struct tr_pipeline_control *second = NULL;
	struct tr_conn_handle first_connection;
	struct tr_conn_handle second_connection;

	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &owner) == TR_OK);
	assert(tr_reactor_start(owner) == TR_OK);

	memset(&registry_config, 0, sizeof(registry_config));
	registry_config.owner = owner;
	registry_config.owner_shard_id = 7U;
	registry_config.capacity = 1U;
	assert(tr_pipeline_registry_create(&registry_config, &registry) == TR_OK);

	memset(&config, 0, sizeof(config));
	config.registry = registry;
	config.pipeline_id = UINT64_C(0x7001);
	config.epoch = UINT64_C(1);
	config.data_capacity = 1U;
	config.stream_affinity_capacity = 2U;

	first_connection = fake_connection(owner, 30U, 1U);
	assert(tr_pipeline_control_create(
		       &config, first_connection, &first) == TR_OK);
	assert(first != NULL);

	/* 容量拒绝必须在候选 Pipeline 绑定 CONTROL 前发生。
	 * 因此创建失败不需要生命周期回滚，可以立即销毁其私有软状态。
	 */
	config.pipeline_id = UINT64_C(0x7002);
	second_connection = fake_connection(owner, 31U, 1U);
	assert(tr_pipeline_control_create(
		       &config, second_connection, &second) == TR_AGAIN);
	assert(second == NULL);

	memset(&stats, 0, sizeof(stats));
	assert(tr_pipeline_registry_get_stats(registry, &stats) == TR_OK);
	assert(stats.count == 1U);

	assert(tr_pipeline_control_close(first, first_connection) == TR_OK);
	first = NULL;
	tr_pipeline_registry_destroy(registry);
	assert(tr_reactor_stop(owner) == TR_OK);
	assert(tr_reactor_destroy(owner) == TR_OK);
}

static void
test_pipeline_control_abort_retires_attached_data_and_epoch_routes(void)
{
	struct tr_reactor *owner = NULL;
	struct tr_pipeline_registry_config registry_config;
	struct tr_pipeline_registry *registry = NULL;
	struct tr_pipeline_registry_stats registry_stats;
	struct tr_pipeline_control_config control_config;
	struct tr_pipeline_control *control = NULL;
	struct tr_pipeline_control *next_control = NULL;
	struct tr_pipeline_stats pipeline_stats;
	struct tr_pipeline_data_offer offers[3];
	struct tr_pipeline_data_offer next_offer;
	struct tr_pipeline_transfer_ready ready;
	struct tr_pipeline_data_ref attached[2];
	struct tr_conn_handle control_connection;
	struct tr_conn_handle next_control_connection;
	struct tr_conn_handle data_connections[2];
	struct tr_conn_handle next_data_connection;
	unsigned i;

	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &owner) == TR_OK);
	assert(tr_reactor_start(owner) == TR_OK);

	memset(&registry_config, 0, sizeof(registry_config));
	registry_config.owner = owner;
	registry_config.owner_shard_id = 9U;
	registry_config.capacity = 1U;
	assert(tr_pipeline_registry_create(&registry_config, &registry) ==
	       TR_OK);

	memset(&control_config, 0, sizeof(control_config));
	control_config.registry = registry;
	control_config.pipeline_id = UINT64_C(0x9001);
	control_config.epoch = UINT64_C(41);
	control_config.data_capacity = 3U;
	control_config.stream_affinity_capacity = 2U;
	control_connection = fake_connection(owner, 50U, 1U);
	assert(tr_pipeline_control_create(&control_config, control_connection,
	                                  &control) == TR_OK);

	for (i = 0U; i < 3U; ++i)
		assert(tr_pipeline_control_reserve_data(control, &offers[i]) ==
		       TR_OK);
	for (i = 0U; i < 2U; ++i) {
		data_connections[i] = fake_connection(owner, 51U + i, 1U);
		assert(tr_pipeline_registry_attach_data_route(
		               registry, &offers[i].route, data_connections[i],
		               &attached[i]) == TR_OK);
		assert(tr_pipeline_control_prepare_transfer(control, 9001U + i,
		                                            &ready) == TR_OK);
		assert(ready.stream_id == 9001U + i);
		assert(ready.data.index == attached[i].index);
		assert(ready.data.generation == attached[i].generation);
	}

	memset(&pipeline_stats, 0, sizeof(pipeline_stats));
	assert(tr_pipeline_control_get_stats(control, &pipeline_stats) ==
	       TR_OK);
	assert(pipeline_stats.control_bound == 1);
	assert(pipeline_stats.data_count == 2U);
	assert(pipeline_stats.data_reserved_count == 1U);
	assert(pipeline_stats.stream_affinity_count == 2U);

	/* 致命 CONTROL 拆除须先清除全部 DATA 代次与亲和，再注销旧 epoch。 */
	assert(tr_pipeline_control_abort(control, control_connection) == TR_OK);
	control = NULL;
	memset(&registry_stats, 0, sizeof(registry_stats));
	assert(tr_pipeline_registry_get_stats(registry, &registry_stats) ==
	       TR_OK);
	assert(registry_stats.count == 0U);
	assert(tr_pipeline_registry_attach_data_route(
	               registry, &offers[0].route,
	               fake_connection(owner, 54U, 1U), NULL) == TR_ERR_STALE);
	assert(tr_pipeline_registry_attach_data_route(
	               registry, &offers[2].route,
	               fake_connection(owner, 58U, 1U), NULL) == TR_ERR_STALE);

	/* 同一 pipeline id 进入新 epoch 后，旧 DATA 前导不得消耗新预留。 */
	control_config.epoch = UINT64_C(42);
	next_control_connection = fake_connection(owner, 55U, 1U);
	assert(tr_pipeline_control_create(&control_config,
	                                  next_control_connection,
	                                  &next_control) == TR_OK);
	assert(tr_pipeline_control_reserve_data(next_control, &next_offer) ==
	       TR_OK);
	assert(next_offer.route.epoch == UINT64_C(42));
	assert(next_offer.route.member_generation >
	       offers[2].route.member_generation);
	assert(tr_pipeline_registry_attach_data_route(
	               registry, &offers[0].route,
	               fake_connection(owner, 56U, 1U), NULL) == TR_ERR_STALE);
	assert(tr_pipeline_registry_attach_data_route(
	               registry, &offers[2].route,
	               fake_connection(owner, 59U, 1U), NULL) == TR_ERR_STALE);

	memset(&pipeline_stats, 0, sizeof(pipeline_stats));
	assert(tr_pipeline_control_get_stats(next_control, &pipeline_stats) ==
	       TR_OK);
	assert(pipeline_stats.data_reserved_count == 1U);
	assert(pipeline_stats.data_count == 0U);
	next_data_connection = fake_connection(owner, 57U, 1U);
	assert(tr_pipeline_registry_attach_data_route(
	               registry, &next_offer.route, next_data_connection,
	               NULL) == TR_OK);
	assert(tr_pipeline_registry_detach_data_route(
	               registry, &next_offer.route, next_data_connection) ==
	       TR_OK);
	assert(tr_pipeline_control_close(next_control,
	                                 next_control_connection) == TR_OK);
	next_control = NULL;
	memset(&registry_stats, 0, sizeof(registry_stats));
	assert(tr_pipeline_registry_get_stats(registry, &registry_stats) ==
	       TR_OK);
	assert(registry_stats.count == 0U);

	tr_pipeline_registry_destroy(registry);
	assert(tr_reactor_stop(owner) == TR_OK);
	assert(tr_reactor_destroy(owner) == TR_OK);
}

static void test_pipeline_control_validation(void)
{
	struct tr_pipeline_control_config config;
	struct tr_pipeline_control *control =
		(struct tr_pipeline_control *)(uintptr_t)1U;
	struct tr_conn_handle connection;

	memset(&config, 0, sizeof(config));
	memset(&connection, 0, sizeof(connection));
	assert(tr_pipeline_control_create(
		       &config, connection, &control) == TR_ERR_INVALID);
	assert(control == NULL);
}

static void test_data_capacity_rejection_does_not_consume_generation(void)
{
	struct tr_reactor *owner = NULL;
	struct tr_pipeline_registry_config registry_config;
	struct tr_pipeline_registry *registry = NULL;
	struct tr_pipeline_control_config config;
	struct tr_pipeline_control *control = NULL;
	struct tr_pipeline_data_offer first;
	struct tr_pipeline_data_offer rejected;
	struct tr_pipeline_data_offer second;
	struct tr_conn_handle connection;

	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &owner) == TR_OK);
	assert(tr_reactor_start(owner) == TR_OK);

	memset(&registry_config, 0, sizeof(registry_config));
	registry_config.owner = owner;
	registry_config.owner_shard_id = 8U;
	registry_config.capacity = 1U;
	assert(tr_pipeline_registry_create(&registry_config, &registry) ==
	       TR_OK);

	memset(&config, 0, sizeof(config));
	config.registry = registry;
	config.pipeline_id = UINT64_C(0x8001);
	config.epoch = UINT64_C(1);
	config.data_capacity = 1U;
	config.stream_affinity_capacity = 1U;
	connection = fake_connection(owner, 40U, 1U);
	assert(tr_pipeline_control_create(&config, connection, &control) ==
	       TR_OK);

	memset(&first, 0, sizeof(first));
	assert(tr_pipeline_control_reserve_data(control, &first) == TR_OK);
	assert(first.data.generation == 1U);

	memset(&rejected, 0xA5, sizeof(rejected));
	assert(tr_pipeline_control_reserve_data(control, &rejected) ==
	       TR_AGAIN);
	assert(tr_pipeline_control_cancel_data(control, &first) == TR_OK);

	memset(&second, 0, sizeof(second));
	assert(tr_pipeline_control_reserve_data(control, &second) == TR_OK);
	assert(second.data.index == first.data.index);
	assert(second.data.generation == 2U);
	assert(second.route.member_generation == 2U);
	assert(tr_pipeline_control_cancel_data(control, &second) == TR_OK);

	assert(tr_pipeline_control_close(control, connection) == TR_OK);
	tr_pipeline_registry_destroy(registry);
	assert(tr_reactor_stop(owner) == TR_OK);
	assert(tr_reactor_destroy(owner) == TR_OK);
}

int main(void)
{
	test_pipeline_control_validation();
	test_pipeline_control_registry_capacity_failure_is_precommit();
	test_pipeline_control_abort_retires_attached_data_and_epoch_routes();
	test_data_capacity_rejection_does_not_consume_generation();
	test_pipeline_control_session();
	puts("pipeline control/transfer-ready: ok");
	return 0;
}
