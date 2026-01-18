import { NextResponse } from "next/server"

export async function GET(
  request: Request,
  context: { params: { id: string } }
) {
  const { id } = context.params
  const url = new URL(request.url)
  const orderId = (id || "").trim()

  if (orderId) {
    return NextResponse.redirect(new URL(`/order/${orderId}`, url))
  }

  return NextResponse.redirect(new URL("/", url))
}
