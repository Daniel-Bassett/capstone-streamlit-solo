import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import polars as pl

######### DEFINE FUNCTIONS ######### 

def load_categories():
    query = """
    with category_counts as (
    select
        category,
        count(category) as count
    from read_parquet('data/all/all_establishments.parquet') 
    where state = 'NY'
    group by category
    )

    select distinct category
    FROM category_counts
    where count >= 50
    order by category asc
    """ 
    return duckdb.query(query).df()


######### LOAD DATA ######### 
establishments = pl.scan_parquet('data/all/all_establishments.parquet')
reviews = pl.scan_parquet('data/all/all_reviews.parquet')
nyc_establishments = (establishments
                      .filter(
                          (True==True)
                          & (pl.col('state') == "NY")
                          & (pl.col('longitude').is_not_null()))
                      )


######### FILTERS #########
categories = st.multiselect(label='Choose categories', options=load_categories())

with st.popover('Filters', use_container_width=True):
    col1, col2= st.columns([3, 3])
    with col1:
        st.write('test')
    with col2:
        st.write('test')



######### MAP #########
map_df = nyc_establishments.collect().to_pandas()
if categories:
    map_df = map_df.query('category.isin(@categories)')

color_min = map_df['average_rating'].quantile(0.025)
color_max = map_df['average_rating'].quantile(0.85)
map_selection = st.plotly_chart(
    px.scatter_map(
        data_frame=map_df, 
        lat='latitude', 
        lon='longitude',
        zoom=12,
        center=dict(lat=40.7473666, lon=-73.9902979),
        color='average_rating',
        color_continuous_scale="RdYlGn",
        range_color=[color_min, color_max],
        map_style='carto-darkmatter',
        hover_name='restaurant_name',
        custom_data=['restaurant_name', 'average_rating', 'score']
        ).update_traces(
            hovertemplate=('%{customdata[0]}<br>'
                            'GoogleMaps Rating: %{customdata[1]}<br>'
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
    st.write(len(map_selection.selection['point_indices']))