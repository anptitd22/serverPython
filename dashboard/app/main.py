from fastapi import FastAPI, Query
from app.database import get_connection
from typing import List, Dict
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
# Cho phép frontend gọi API
origins = [
    "*",  # Hoặc domain thực tế từ ngrok
    "http://localhost:5173",
    "https://d1d64c951cf6203d6756a3d86b039ebf.serveo.net"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,              # Cho phép domain frontend
    allow_credentials=True,
    allow_methods=["*"],                # Cho phép tất cả method: GET, POST, OPTIONS,...
    allow_headers=["*"],                # Cho phép tất cả headers
)


@app.get("/revenue/weekly")
def get_weekly_revenue() -> List[Dict]:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
        dd.year, 
        dd.week, 
        SUM(fs.revenue) AS total_revenue, 
        SUM(fs.profit) AS total_profit
    FROM fact_sales fs
    JOIN dim_date dd ON fs.date_key = dd.date_key
    WHERE dd.date >= DATEADD(WEEK, -10, GETDATE())  -- Lấy 10 tuần gần nhất
    GROUP BY dd.year, dd.week
    ORDER BY dd.year ASC, dd.week ASC;
    """)
    rows = cursor.fetchall()
    return [
        {"year": row[0], "week": row[1], "total_revenue": float(row[2]), "total_profit": float(row[3])}
        for row in rows
    ]

@app.get("/revenue/monthly")
def get_monthly_revenue() -> List[Dict]:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
        dd.year, 
        dd.month, 
        SUM(fs.revenue) AS total_revenue, 
        SUM(fs.profit) AS total_profit
    FROM fact_sales fs
    JOIN dim_date dd ON fs.date_key = dd.date_key
    WHERE dd.date >= DATEADD(MONTH, -10, GETDATE())  -- Lấy 10 tháng gần nhất
    GROUP BY dd.year, dd.month
    ORDER BY dd.year ASC, dd.month ASC;
    """)
    rows = cursor.fetchall()
    return [
        {"year": row[0], "month": row[1], "total_revenue": float(row[2]), "total_profit": float(row[3])}
        for row in rows
    ]

@app.get("/revenue/yearly")
def get_yearly_revenue() -> List[Dict]:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
        dd.year, 
        SUM(fs.revenue) AS total_revenue, 
        SUM(fs.profit) AS total_profit
    FROM fact_sales fs
    JOIN dim_date dd ON fs.date_key = dd.date_key
    WHERE dd.date >= DATEADD(YEAR, -3, GETDATE())  -- Lấy 3 năm gần nhất
    GROUP BY dd.year
    ORDER BY dd.year ASC;
    """)
    rows = cursor.fetchall()
    return [
        {"year": row[0], "total_revenue": float(row[1]), "total_profit": float(row[2])}
        for row in rows
    ]
@app.get("/top-products")
def get_top_products() -> List[Dict]:
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT TOP 5 
                p.name AS product_name,
                SUM(f.quantity) AS total_quantity_sold,
                SUM(f.total_price) AS total_revenue
            FROM fact_sales f
            JOIN dim_product p ON f.product_key  = p.product_key 
            GROUP BY p.name
            ORDER BY total_quantity_sold DESC;
        """)
        rows = cursor.fetchall()

        return [
            {
                "product_name": row[0],
                "total_quantity_sold": row[1],
                "total_revenue": float(row[2])
            }
            for row in rows
        ]
    except Exception as e:
        return {"error": str(e)}

@app.get("/top-customers")
def get_top_customers() -> List[Dict]:
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT TOP 5
                c.client_id,
                c.name,
                c.email,
                c.phone,
                c.address,
                SUM(f.total_price) AS total_spent,
                COUNT(DISTINCT f.order_id) AS num_orders
            FROM fact_sales f
            JOIN dim_customer c ON f.customer_key  = c.customer_key 
            GROUP BY c.client_id, c.name, c.email, c.phone, c.address
            ORDER BY total_spent DESC;
        """)
        rows = cursor.fetchall()

        return [
            {
                "client_id": row[0],
                "name": row[1],
                "email": row[2],
                "phone": row[3],
                "address": row[4],
                "total_spent": float(row[5]),
                "num_orders": row[6]
            }
            for row in rows
        ]
    except Exception as e:
        return {"error": str(e)}

@app.get("/low-stock-count")
def get_low_stock_count(threshold: int = Query(10, description="Ngưỡng số lượng để xác định sản phẩm sắp hết hàng")) -> Dict:
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT COUNT(*) 
            FROM dim_product 
            WHERE stock <= ?;
        """, threshold)

        count = cursor.fetchone()[0]
        return {"low_stock_count": count}
    except Exception as e:
        return {"error": str(e)}

@app.get("/order-status-stats")
def get_order_status_stats() -> Dict:
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Câu truy vấn SQL
        cursor.execute("""
            SELECT
                status,
                COUNT(*) AS total,
                ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM dim_order WHERE status IN ('DELIVERED', 'CANCELLED')), 2) AS percentage
            FROM
                dim_order
            WHERE
                status IN ('DELIVERED', 'CANCELLED')
            GROUP BY
                status;
        """)

        # Lấy dữ liệu từ kết quả truy vấn
        result = cursor.fetchall()

        # Chuyển kết quả sang định dạng dictionary
        status_stats = [
            {"status": row[0], "total": row[1], "percentage": row[2]}
            for row in result
        ]

        return {"order_status_stats": status_stats}

    except Exception as e:
        return {"error": str(e)}

    finally:
        conn.close()
