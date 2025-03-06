import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(layout='wide')

# Load Data
establishments_df = pd.read_parquet('data/atx_establishments.parquet').dropna(subset='n_reviews').astype({'latitude': float, 'longitude': float})
duckdb.register('establishments', establishments_df)

# query = """
# select 
#     facility_id,
#     timestamp,
#     rating,
#     text
# from read_csv_auto('data/atx_reviews.csv')
# where text not null
# """
# reviews_df = duckdb.query(query).df()

st.title('KITCHEN NIGHTMARES')

col1, col2 = st.columns([6, 6])

with col1:
    map_df = establishments_df.copy()

    temp_cols = st.columns([6, 6])


    # categories filter
    with temp_cols[0]:
        categories_filter = st.multiselect(label='Choose category', options=map_df.category.unique().tolist())
        if categories_filter: map_df = map_df.query('category.isin(@categories_filter)')

    # scale var 
    with temp_cols[1]:
        scale_var = st.selectbox(label='Choose Scale Variable', options=['score', 'average_rating'])
        color_min = establishments_df[scale_var].quantile(0.025)
        color_max = establishments_df[scale_var].quantile(0.85)

    # create map
    map_selection = st.plotly_chart(
        px.scatter_map(
            data_frame=map_df, 
            lat='latitude', 
            lon='longitude',
            zoom=12,
            center=dict(lat=30.26, lon=-97.74),
            color=scale_var,
            color_continuous_scale="RdYlGn",
            range_color=[color_min, color_max],
            map_style='carto-darkmatter',
            hover_name='restaurant_name',
            custom_data=['restaurant_name', 'average_rating', 'score']
            ).update_traces(
                hovertemplate=('%{customdata[0]}<br>'
                               'GoogleMaps Rating: %{customdata[1]}<br>'
                               'Inspection Score: %{customdata[2]}<br>'
                               
                               ),
            ).update_layout(
                width=800,
                height=800
            ).update_coloraxes(
                showscale=False
            )
            , 
        on_select='rerun',
        use_container_width=True
        )



if map_selection.selection['point_indices']:
    temp_df = map_df.iloc[map_selection.selection['point_indices']]
    with col2:
        df_data = st.dataframe(temp_df[['restaurant_name', 'score', 'average_rating', 'category', 'price', 'facility_id']], 
                               on_select="rerun", 
                               selection_mode="multi-row", 
                               hide_index=True
                               )
        
        fac_ids = tuple(temp_df.iloc[df_data['selection']['rows']]['facility_id'].to_list())

        query = f"""
        select 
            facility_id,
            timestamp,
            rating,
            text
        from read_csv_auto('data/atx_reviews.csv')
        where true
        and facility_id in {fac_ids}
        and text not null
        """
        reviews_df = duckdb.query(query).df()
        st.write(reviews_df.sort_values(by='timestamp'))

        # st.write(df_data)
        # st.write(temp_df.iloc[df_data['selection']['rows']]['facility_id'])
        # filter_cols = st.columns([6, 6])
        # with filter_cols[0]:
        #     agg_col = st.selectbox(label='Choose Aggregation', options=['average_rating', 'n_reviews', 'score'])
        # with filter_cols[1]:
        #     top_n = st.number_input(label='Top N', min_value=1, max_value=len(temp_df.category.unique()), value=len(temp_df.category.unique()[:10]))
    
        # # BAR PLOT
        # agg_df = (temp_df
        #         #   .query('category.isin(@category_filter)')
        #           .groupby('category')
        #           [agg_col]
        #           .mean()
        #           .sort_values()
        #           .iloc[:top_n]
        #           )
        # category = st.plotly_chart(
        #     px.bar(agg_df,
        #            orientation='h',
        #            ),
        #     selection_mode="points",
        #     on_select='rerun'
        # )
    


        # if category.selection['point_indices']:
        #     star_range = st.slider(label='Select Star Range', min_value=1, max_value=5, value=(1, 5))
        #     st.write(star_range)
        #     cat = agg_df.iloc[category.selection['point_indices']].index
        #     single_category_df = temp_df.query('category == @cat[0]').sort_values(by='average_rating', ascending=False)
        #     fac_ids = tuple(single_category_df.facility_id.unique().tolist())
        #     query = f"""
        #     select 
        #         facility_id,
        #         timestamp,
        #         rating,
        #         text
        #     from read_csv_auto('data/atx_reviews.csv')
        #     where true
        #     and facility_id in {fac_ids}
        #     and text not null
        #     """
        #     reviews_df = duckdb.query(query).df()
        #     st.write(reviews_df.query('rating.between(@star_range[0], @star_range[1])').sort_values(by='timestamp'))
        




