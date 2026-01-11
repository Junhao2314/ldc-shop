'use server'

import { db } from "@/lib/db"
import { cards, orders, refundRequests, loginUsers } from "@/lib/db/schema"
import { and, eq, sql } from "drizzle-orm"
import { revalidatePath } from "next/cache"
import { checkAdmin } from "@/actions/admin"

export async function markOrderPaid(orderId: string) {
  await checkAdmin()
  if (!orderId) throw new Error("Missing order id")

  await db.update(orders).set({
    status: 'paid',
    paidAt: new Date(),
  }).where(eq(orders.orderId, orderId))

  revalidatePath('/admin/orders')
  revalidatePath(`/admin/orders/${orderId}`)
  revalidatePath(`/order/${orderId}`)
}

export async function markOrderDelivered(orderId: string) {
  await checkAdmin()
  if (!orderId) throw new Error("Missing order id")

  const order = await db.query.orders.findFirst({ where: eq(orders.orderId, orderId) })
  if (!order) throw new Error("Order not found")
  if (!order.cardKey) throw new Error("Missing card key; cannot mark delivered")

  await db.update(orders).set({
    status: 'delivered',
    deliveredAt: new Date(),
  }).where(eq(orders.orderId, orderId))

  revalidatePath('/admin/orders')
  revalidatePath(`/admin/orders/${orderId}`)
  revalidatePath(`/order/${orderId}`)
}

export async function cancelOrder(orderId: string) {
  await checkAdmin()
  if (!orderId) throw new Error("Missing order id")

  await db.transaction(async (tx: any) => {
    // 1. Refund points if used
    const order = await tx.query.orders.findFirst({
      where: eq(orders.orderId, orderId),
      columns: { userId: true, pointsUsed: true }
    })

    if (order?.userId && order.pointsUsed && order.pointsUsed > 0) {
      await tx.update(loginUsers)
        .set({ points: sql`${loginUsers.points} + ${order.pointsUsed}` })
        .where(eq(loginUsers.userId, order.userId))
    }

    await tx.update(orders).set({ status: 'cancelled' }).where(eq(orders.orderId, orderId))
    try {
      await tx.execute(sql`
        ALTER TABLE cards ADD COLUMN IF NOT EXISTS reserved_order_id TEXT;
        ALTER TABLE cards ADD COLUMN IF NOT EXISTS reserved_at TIMESTAMP;
      `)
    } catch {
      // best effort
    }
    await tx.update(cards).set({ reservedOrderId: null, reservedAt: null })
      .where(sql`${cards.reservedOrderId} = ${orderId} AND ${cards.isUsed} = false`)
  })

  revalidatePath('/admin/orders')
  revalidatePath(`/admin/orders/${orderId}`)
  revalidatePath(`/order/${orderId}`)
}

export async function updateOrderEmail(orderId: string, email: string | null) {
  await checkAdmin()
  if (!orderId) throw new Error("Missing order id")
  const next = (email || '').trim()
  await db.update(orders).set({ email: next || null }).where(eq(orders.orderId, orderId))
  revalidatePath('/admin/orders')
  revalidatePath(`/admin/orders/${orderId}`)
}

async function deleteOneOrder(tx: any, orderId: string) {
  const order = await tx.query.orders.findFirst({ where: eq(orders.orderId, orderId) })
  if (!order) return

  // Refund points if used
  if (order.userId && order.pointsUsed && order.pointsUsed > 0) {
    await tx.update(loginUsers)
      .set({ points: sql`${loginUsers.points} + ${order.pointsUsed}` })
      .where(eq(loginUsers.userId, order.userId))
  }

  // Release reserved card if any
  try {
    await tx.execute(sql`
      ALTER TABLE cards ADD COLUMN IF NOT EXISTS reserved_order_id TEXT;
      ALTER TABLE cards ADD COLUMN IF NOT EXISTS reserved_at TIMESTAMP;
    `)
  } catch {
    // best effort
  }

  await tx.update(cards).set({ reservedOrderId: null, reservedAt: null })
    .where(sql`${cards.reservedOrderId} = ${orderId} AND ${cards.isUsed} = false`)

  // Delete related refund requests (best effort)
  try {
    await tx.delete(refundRequests).where(eq(refundRequests.orderId, orderId))
  } catch {
    // table may not exist yet
  }

  await tx.delete(orders).where(eq(orders.orderId, orderId))
}

export async function deleteOrder(orderId: string) {
  await checkAdmin()
  if (!orderId) throw new Error("Missing order id")

  await db.transaction(async (tx: any) => {
    await deleteOneOrder(tx, orderId)
  })

  revalidatePath('/admin/orders')
  revalidatePath(`/admin/orders/${orderId}`)
}

export async function deleteOrders(orderIds: string[]) {
  await checkAdmin()
  const ids = (orderIds || []).map((s) => String(s).trim()).filter(Boolean)
  if (!ids.length) return

  await db.transaction(async (tx: any) => {
    for (const id of ids) {
      await deleteOneOrder(tx, id)
    }
  })

  revalidatePath('/admin/orders')
}

import { queryOrderStatus } from "@/lib/epay"

export async function verifyOrderRefundStatus(orderId: string) {
  await checkAdmin()
  if (!orderId) throw new Error("Missing order id")

  try {
    const result = await queryOrderStatus(orderId)

    if (result.success) {
      // status 0 = Refunded
      if (result.status === 0) {
        await db.update(orders).set({ status: 'refunded' }).where(eq(orders.orderId, orderId))
        revalidatePath('/admin/orders')
        return { success: true, status: result.status, msg: 'Refunded (Verified)' }
      } else if (result.status === 1) {
        return { success: true, status: result.status, msg: 'Paid (Not Refunded)' }
      } else {
        return { success: true, status: result.status, msg: `Status: ${result.status}` }
      }
    } else {
      return { success: false, error: result.error }
    }

  } catch (e: any) {
    console.error('Verify refund error', e)
    return { success: false, error: e.message }
  }
}
