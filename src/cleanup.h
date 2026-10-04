#ifndef TR_CLEANUP_H
#define TR_CLEANUP_H

#include <stddef.h>

/*
 * c-trpc 以 ISO C11 为语言基线。作用域自动清理是项目有意使用的
 * 编译器扩展；GCC 和 Clang 在 C11 模式下都支持 cleanup attribute。
 *
 * cleanup 变量默认拥有对应资源，直到显式调用 typed take() helper
 * 清空变量并转移 ownership。这样所有 return/goto 路径都能自动释放
 * 仍由当前作用域持有的资源，同时不会隐藏 ownership transfer。
 */
#if defined(__GNUC__) || defined(__clang__)
#define TR_AUTO(cleanup_fn) __attribute__((cleanup(cleanup_fn)))
#else
#error "c-trpc requires GCC/Clang cleanup attribute support"
#endif

/*
 * 为指针资源定义 typed ownership helper。
 *
 * release_fn() 必须接收 "type *"，并能释放一个有效资源。
 * cleanup() 在离开作用域时执行；take() 用于显式把 ownership
 * 从 cleanup-managed 变量转移出去。
 */
#define TR_DEFINE_PTR_OWNERSHIP(prefix, type, release_fn)               \
	static inline void prefix##_cleanup(type **resource)              \
	{                                                                \
		if (resource && *resource) {                               \
			release_fn(*resource);                              \
			*resource = NULL;                                   \
		}                                                        \
	}                                                                \
	static inline type *prefix##_take(type **resource)                \
	{                                                                \
		type *value = resource ? *resource : NULL;                 \
		if (resource)                                             \
			*resource = NULL;                                   \
		return value;                                             \
	}

#endif
