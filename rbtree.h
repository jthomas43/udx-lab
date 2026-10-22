#ifndef rbt_h_INCLUDED
#define rbt_h_INCLUDED
#include <stdint.h>

typedef struct rbtree_node_s rbtree_node_t;

struct rbtree_node_s {
    uint32_t key;
    rbtree_node_t *left;
    rbtree_node_t *right;
    rbtree_node_t *parent;
    uint8_t color;
    uint8_t data;
};

typedef void (*rbtree_insert_fn)(rbtree_node_t *root, rbtree_node_t *node,
                                 rbtree_node_t *sentinel);

typedef struct {
    rbtree_node_t *root;
    rbtree_node_t *sentinel;
    rbtree_insert_fn insert;
    rbtree_node_t *min; // cached left-most node
} rbtree_t;

// aside - compiler stuff
// we could tag a struct type as intrusive

#define rbt_data(node, type, link) \
    (type *)((uint8_t *)(node) - offsetof(type, link))

void rbtree_init(rbtree_t *tree, rbtree_node_t *sentinel,
                 rbtree_insert_fn insert_fn);
void rbtree_insert(rbtree_t *tree, rbtree_node_t *node);
void rbtree_delete(rbtree_t *tree, rbtree_node_t *node);
void rbtree_insert_value(rbtree_node_t *root, rbtree_node_t *node,
                         rbtree_node_t *sentinel);
void rbtree_insert_wrapping_value(rbtree_node_t *root, rbtree_node_t *node,
                                  rbtree_node_t *sentinel);

static inline rbtree_node_t *rbtree_min(rbtree_node_t *node,
                                        rbtree_node_t *sentinel) {
    while (node->left != sentinel) {
        node = node->left;
    }
    return node;
}

rbtree_node_t *rbtree_next(rbtree_t *tree, rbtree_node_t *node);

#define rbt_red(node) ((node)->color = 1)
#define rbt_black(node) ((node)->color = 0)
#define rbt_is_red(node) ((node)->color)
#define rbt_is_black(node) (!rbt_is_red(node))
#define rbt_copy_color(n1, n2) (n1->color = n2->color)

#endif // rbt_h_INCLUDED
