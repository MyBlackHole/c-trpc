#ifndef TR_CLEANUP_H
#define TR_CLEANUP_H

/*
 * c-trpc uses ISO C11 as its language baseline. Scope cleanup is the one
 * intentionally required compiler extension: GCC and Clang both support the
 * cleanup attribute in C11 mode.
 *
 * Cleanup variables own their resource until an explicit typed take() helper
 * clears that variable. Cleanup therefore protects every return/goto path
 * without hiding ownership transfer.
 */
#if defined(__GNUC__) || defined(__clang__)
#define TR_AUTO(cleanup_fn) __attribute__((cleanup(cleanup_fn)))
#else
#error "c-trpc requires GCC/Clang cleanup attribute support"
#endif

/*
 * Define ownership helpers for pointer resources.
 *
 * release_fn() must accept "type *" and must tolerate a live resource.
 * cleanup() runs at scope exit; take() explicitly transfers ownership out of
 * the cleanup-managed variable.
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
