





fn new_order(tx: &mut TransactionOCC, 
             tables: TablesRef,
             w_id: i32,
             d_id: i32,
             c_id: i32,
             ol_cnt: i32,
             src_whs : &[i32],
             item_ids: &[i32],
             qty: &[i32],
             now: i64)
)
{
    let w_tax = tables.warehouse_table.retrieve(w_id).unwrap().read(tx).w_tax;
    let c_discount = tables.customer_table.retrieve((w_id, d_id, c_id)).unwrap().read(tx).c_discount;
    let district_ref = tables.district_table.retrieve((w_id, d_id)).unwrap();
    let district = district_ref.read(tx);
    let o_id = district.d_next_o_id;
    let d_tax = district.d_tax;
    let mut district_new = district.clone();
    district_new.d_next_o_id = o_id +1;
    district_ref.write(tx, district_new);

    let mut all_local :i64 = 1;
    for i in 0..ol_cnt as usize {
        if w_id != supware[i] {
            all_local = 0;
        }
    }
     
    tables.order_table.push(tx,
                            Order {
                                o_id: o_id, o_d_id: d_id, o_w_id: w_id, o_c_id: c_id, o_entry_d: now,
                                o_carrier_id: 0, o_ol_cnt: Numeric::new(ol_cnt as i64, 1, 0),
                                o_all_local: Numeric::new(all_local, 1, 0)
                            });
    tables.neworder_table.push(tx,
                               Neworder { no_o_id: o_id, no_d_id: d_id, no_w_id: w_id });

    for i in 0..ol_cnt as usize {
        let i_price = tables.item_table.retrieve(itemid[i]).unwrap().read(tx).i_price;

        let stock_ref = tables.stock_table.retrieve((supware[i], itemid[i])).unwrap();
        let stock = stock_ref.read(tx);
        let s_quantity = stock.s_quantity;
        let s_remote_cnt = stock.s_remote_cnt;
        let s_order_cnt = stock.s_order_cnt;
        let s_dist = match d_id {
            1 => &stock.s_dist_01,
            2 => &stock.s_dist_02,
            3 => &stock.s_dist_03,
            4 => &stock.s_dist_04,
            5 => &stock.s_dist_05,
            6 => &stock.s_dist_06,
            7 => &stock.s_dist_07,
            8 => &stock.s_dist_08,
            9 => &stock.s_dist_09,
            10 => &stock.s_dist_10,
            _ => panic!("invalid d_id: {}", d_id)
        };

        let qty = Numeric::new(qty[i] as i64, 4, 0);
        let mut stock_new = stock.clone();
        stock_new.s_quantity = if s_quantity > qty {
            stock_new.s_quantity - qty
        } else {
            stock_new.s_quantity + Numeric::new(91, 4, 0) - qty
        };

        if supware[i] != w_id {
            stock_new.s_remote_cnt = stock.s_remote_cnt + s_remote_cnt;
        } else {
            stock_new.s_order_cnt = s_order_cnt + Numeric::new(1, 4, 0);
        }

        stock_ref.write(tx, stock_new);

        let ol_amount = qty * i_price * (Numeric::new(1, 1, 0) + w_tax + d_tax) *
            (Numeric::new(1, 1, 0) - c_discount);

        tables.orderline_table.push(tx, Orderline {
            ol_o_id: o_id, ol_d_id: d_id, ol_w_id: w_id, ol_number: i as i32 + 1, ol_i_id: itemid[i],
            ol_supply_w_id: supware[i], ol_delivery_d: 0, ol_quantity: qty, ol_amount: ol_amount,
            ol_dist_info: s_dist.clone()
        })
    }

}
