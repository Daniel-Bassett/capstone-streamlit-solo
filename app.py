import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import polars as pl

st.set_page_config(layout='wide')

test_kw = ['cozy',
 'gem',
 'cute',
 'incredible',
 'unique',
 'ambiance',
 'hidden',
 'lovely',
 'delightful',
 'phenomenal',
 'vibes',
 'creative',
 'vibe',
 'horrible',
 'nasty',
 'inviting',
 'beautifully',
 'disgusting',
 'decor',
 'rich',
 'balanced',
 'refreshing',
 'fantastic',
 'charming',
 'ambience',
 'notch',
 'recommendations',
 'worst',
 'beautiful']

test_kw = '|'.join(test_kw)

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


def load_filtered_reviews(fac_ids):
    query = f"""
    SELECT
        facility_id,
        text,
        rating
    FROM read_parquet('data/all/all_reviews.parquet')
    WHERE 
        True
        AND facility_id IN {fac_ids}    
        AND REGEXP_MATCHES(text, '{test_kw}')
        AND text NOT NULL
    """
    return duckdb.query(query).df()


######### LOAD DATA ######### 
establishments = pl.scan_parquet('data/all/all_establishments.parquet')
reviews = pl.scan_parquet('data/all/all_reviews.parquet')
nyc_establishments = (establishments
                      .filter(
                          (True==True)
                          & (pl.col('state') == "NY")
                          & (pl.col('longitude').is_not_null())
                          & (pl.col('average_rating').is_not_null())
                          )
                      )

######### LAYOUT #########
main_filter_col, _ = st.columns([6, 6])

map_col, agg_col = st.columns([6, 6])

######### FILTERS #########
with main_filter_col:
    with st.popover('Filters', use_container_width=True):
        filter_col1, filter_col2= st.columns([3, 3])
        with filter_col1:
            # categories = st.multiselect(label='Choose categories', options=load_categories())
            categories = st.pills(label='Choose categories', options=load_categories(), selection_mode='multi')
        with filter_col2:
            st.write('placeholder')



######### MAP #########
with map_col:
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
            opacity=0.75,
            map_style='carto-darkmatter',
            hover_name='restaurant_name',
            custom_data=['restaurant_name', 'average_rating', 'score']
            ).update_traces(
                hovertemplate=('%{customdata[0]}<br>'
                                'GoogleMaps Rating: %{customdata[1]}<br>'
                                ),
                marker=dict(size=10)
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

with agg_col:
    tab1, tab2 = st.tabs(["📈 Ratings Over Time", ":star: Reviews"])
    with tab1:
        if map_selection.selection['point_indices']:
            map_selection_idx = map_selection.selection['point_indices']
            fac_ids = map_df.iloc[map_selection_idx]['facility_id'].unique()
        else:
            fac_ids = map_df.facility_id.unique()
        fac_ids = tuple(fac_ids)
        
        ratings_timeseries = (nyc_establishments
                            .filter(pl.col('facility_id').is_in(fac_ids))
                            .select('facility_id')
                            .join(reviews, on='facility_id')
                            .with_columns(pl.col('timestamp').cast(pl.Datetime('us')))
                            .with_columns(pl.col('timestamp').dt.strftime('%Y-%m').alias('year_month'))
                            .filter(pl.col('timestamp').dt.year() >= 2020)
                            .group_by('year_month')
                            .agg(pl.col('rating').mean().alias('monthly_rating'))
                            .sort(by='year_month')
                            .with_columns(rolling_mean=pl.col('monthly_rating').rolling_mean(window_size=6))
                            .collect()
                            )

        st.plotly_chart(
            px.line(
                data_frame=ratings_timeseries,
                x='year_month',
                y='rolling_mean'
            )
        )

    with tab2:
        filtered_reviews = load_filtered_reviews(fac_ids)
        st.write(filtered_reviews)