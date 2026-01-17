import { db } from "@/lib/db";
import { orders, cards, products } from "@/lib/db/schema";
import { eq, sql } from "drizzle-orm";
import { isPaymentOrder } from "@/lib/payment";

export async function processOrderFulfillment(orderId: string, paidAmount: number, tradeNo: string) {
    const order = await db.query.orders.findFirst({
        where: eq(orders.orderId, orderId)
    });

    if (!order) {
        throw new Error(`Order ${orderId} not found`);
    }

    // Verify Amount (Prevent penny-dropping)
    const orderMoney = parseFloat(order.amount);

    // Allow small float epsilon difference
    if (Math.abs(paidAmount - orderMoney) > 0.01) {
        throw new Error(`Amount mismatch! Order: ${orderMoney}, Paid: ${paidAmount}`);
    }

    if (isPaymentOrder(order.productId)) {
        if (order.status === 'pending' || order.status === 'cancelled') {
            await db.update(orders)
                .set({
                    status: 'paid',
                    paidAt: new Date(),
                    tradeNo: tradeNo
                })
                .where(eq(orders.orderId, orderId));
        }
        return { success: true, status: 'processed' };
    }

    if (order.status === 'pending' || order.status === 'cancelled') {
        // Check if product is shared (infinite stock)
        const product = await db.query.products.findFirst({
            where: eq(products.id, order.productId),
            columns: {
                isShared: true
            }
        });

        const isShared = product?.isShared;

        if (isShared) {
            // For shared products:
            // 1. Find ONE available card (unused)
            // 2. Do NOT mark as used
            // 3. Use random selection for load balancing if multiple cards exist
            const availableCard = await db.select({ id: cards.id, cardKey: cards.cardKey })
                .from(cards)
                .where(sql`${cards.productId} = ${order.productId} AND COALESCE(${cards.isUsed}, 0) = 0`)
                .orderBy(sql`RANDOM()`)
                .limit(1);

            if (availableCard.length > 0) {
                // For shared products, we use the same card key for all items
                const key = availableCard[0].cardKey;
                const cardKeys = Array(order.quantity || 1).fill(key);

                // No transaction - D1 doesn't support SQL transactions
                // Update order status to delivered
                await db.update(orders)
                    .set({
                        status: 'delivered',
                        paidAt: new Date(),
                        deliveredAt: new Date(),
                        tradeNo: tradeNo,
                        cardKey: cardKeys.join('\n'), // Store keys separated by newline
                        currentPaymentId: null // Clear payment ID to prevent re-processing
                    })
                    .where(eq(orders.orderId, orderId));

                // Log output for debugging
                console.log(`[Fulfill] Shared product order ${orderId} delivered. Card: ${key}`);

                return { success: true, status: 'processed' };
            } else {
                // If no card is available for a shared product, treat as no stock
                await db.update(orders)
                    .set({ status: 'paid', paidAt: new Date(), tradeNo: tradeNo })
                    .where(eq(orders.orderId, orderId));
                console.log(`[Fulfill] Order ${orderId} marked as paid (no stock for shared product)`);
                return { success: true, status: 'processed' };
            }
        }

        const quantity = order.quantity || 1;

        // No transaction - D1 doesn't support SQL transactions
        let cardKeys: string[] = [];
        const oneMinuteAgo = Date.now() - 60000;

        // 1. First, try to claim reserved cards for this order
        try {
            const reservedCards = await db.select({ id: cards.id, cardKey: cards.cardKey })
                .from(cards)
                .where(sql`${cards.reservedOrderId} = ${orderId} AND COALESCE(${cards.isUsed}, 0) = 0`)
                .limit(quantity);

            for (const card of reservedCards) {
                await db.update(cards)
                    .set({
                        isUsed: true,
                        usedAt: new Date(),
                        reservedOrderId: null,
                        reservedAt: null
                    })
                    .where(eq(cards.id, card.id));
                cardKeys.push(card.cardKey);
            }
        } catch (error: any) {
            // reservedOrderId column might not exist
            console.log('[Fulfill] Reserved cards check failed:', error.message);
        }

        // 2. If we need more cards, claim available ones
        if (cardKeys.length < quantity) {
            const needed = quantity - cardKeys.length;
            console.log(`[Fulfill] Order ${orderId}: Found ${cardKeys.length} reserved cards, need ${needed} more.`);

            const availableCards = await db.select({ id: cards.id, cardKey: cards.cardKey })
                .from(cards)
                .where(sql`${cards.productId} = ${order.productId} AND COALESCE(${cards.isUsed}, 0) = 0 AND (${cards.reservedAt} IS NULL OR ${cards.reservedAt} < ${oneMinuteAgo})`)
                .limit(needed);

            for (const card of availableCards) {
                await db.update(cards)
                    .set({
                        isUsed: true,
                        usedAt: new Date()
                    })
                    .where(eq(cards.id, card.id));
                cardKeys.push(card.cardKey);
            }
        }

        console.log(`[Fulfill] Order ${orderId}: Cards claimed: ${cardKeys.length}/${quantity}`);

        if (cardKeys.length > 0) {
            const joinedKeys = cardKeys.join('\n');

            await db.update(orders)
                .set({
                    status: 'delivered',
                    paidAt: new Date(),
                    deliveredAt: new Date(),
                    tradeNo: tradeNo,
                    cardKey: joinedKeys
                })
                .where(eq(orders.orderId, orderId));
            console.log(`[Fulfill] Order ${orderId} delivered successfully!`);
        } else {
            // Paid but no stock
            await db.update(orders)
                .set({ status: 'paid', paidAt: new Date(), tradeNo: tradeNo })
                .where(eq(orders.orderId, orderId));
            console.log(`[Fulfill] Order ${orderId} marked as paid (no stock)`);
        }
        return { success: true, status: 'processed' };
    } else {
        return { success: true, status: 'already_processed' }; // Idempotent success
    }
}
