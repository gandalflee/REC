select *
  from (
        
        select row_number() over(partition by current_item_category, current_item_id order by source, view_count desc) no,
                current_item_id,
                current_item_category,
                andx_item_id,
                source,
                view_count
          from (
                 
                 select current_item_id,
                         current_item_category,
                         andx_item_id,
                         1 source,
                         view_count
                   from test_x_and_x
                 
                 union
                 
                 --从这里开始，实现了同一item的x and x数据中，来源2和来源1数据的去重
                 --c表就是每个当前item相同分类下的topN，这里的topN有可能和x了又x表的数据有重复，下面就通过左连接去重
                 select c.*
                   from (select a.current_item_id,
                                a.current_item_category,
                                b.item_id,
                                2,
                                b.view_count
                           from (select current_item_id, current_item_category
                                   from test_x_and_x
                                  group by current_item_id, current_item_category) a,
                                test_topn b
                          where a.current_item_category = b.item_category) c
                 
                   left outer join
                 --通过左连接实现去重
                 --用于做补全的数据是左表，现在要去掉左表（source 2）中和右表（source 1）重复的数据
                 --左右表做左连接后，只有左右表重复的那些行是没有null的
                 --所有左表中可用于补充的新数据对应右表的列都是null，所以可以用这个条件来过滤得到和右表不重复的数据
                 
                 test_x_and_x d
                 
                     on (c.current_item_id = d.current_item_id and
                        c.current_item_category = d.current_item_category AND
                        c.item_id = d.andx_item_id)
                  where d.andx_item_id is null
                 
                 )
        
        )
 where no <= 5
