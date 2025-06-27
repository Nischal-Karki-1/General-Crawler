from database.setup import get_connection, return_connection, close_all_connections
import asyncio
import networkx as nx
import plotly.graph_objects as go
import numpy as np
from collections import Counter

async def fetch_url_path_relations(conn):
    """Fetch URL path relationships using the specified query"""
    async with conn.cursor() as cursor:
        await cursor.execute("""
            SELECT 
                p_url.url_path AS parent_url,
                c_url.url_path AS child_url
            FROM url_relationship ur
            JOIN crawled_url p_url ON ur.parent_url_id = p_url.crawl_id
            JOIN crawled_url c_url ON ur.child_url_id = c_url.crawl_id
            WHERE p_url.domain_id = 'domain154' OR c_url.domain_id = 'domain154'
            LIMIT 5000
        """)
        return await cursor.fetchall()

def create_fast_network_graph(relations):
    """Create fast-loading clean network graph"""
    if not relations:
        print("No relations found")
        return None
    
    print(f"Processing {len(relations)} URL path relationships...")
    
    # Create NetworkX graph
    G = nx.DiGraph()
    
    # Add edges (nodes are added automatically)
    for parent_path, child_path in relations:
        if parent_path and child_path:
            G.add_edge(parent_path, child_path)
    
    print(f"Created graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")
    
    if G.number_of_nodes() == 0:
        return None
    
    # Create layout - use simple spring layout
    print("Creating layout...")
    pos = nx.spring_layout(G, k=1, iterations=50, seed=42)
    
    # Calculate node statistics for hover
    node_stats = {}
    for node in G.nodes():
        in_degree = G.in_degree(node)
        out_degree = G.out_degree(node)
        node_stats[node] = {
            'incoming': in_degree,
            'outgoing': out_degree,
            'total': in_degree + out_degree
        }
    
    # Create edges - simple lines only
    edge_x = []
    edge_y = []
    
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])
    
    edge_trace = go.Scatter(
        x=edge_x, 
        y=edge_y,
        mode='lines',
        line=dict(width=1, color='#666666'),
        hoverinfo='skip',
        showlegend=False
    )
    
    # Create nodes - clean and simple
    node_x = []
    node_y = []
    hover_texts = []
    
    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        
        stats = node_stats[node]
        hover_text = f"<b>URL:</b> {node}<br><b>In:</b> {stats['incoming']}<br><b>Out:</b> {stats['outgoing']}<br><b>Total:</b> {stats['total']}"
        hover_texts.append(hover_text)
    
    node_trace = go.Scatter(
        x=node_x,
        y=node_y,
        mode='markers',
        marker=dict(
            size=8,
            color='#1f77b4',
            line=dict(width=1, color='white')
        ),
        hoverinfo='text',
        hovertext=hover_texts,
        showlegend=False
    )
    
    # Create simple figure - NO ARROWS to avoid slowdown
    fig = go.Figure(
        data=[edge_trace, node_trace],
        layout=go.Layout(
            title=f"URL Network - {G.number_of_nodes()} nodes, {G.number_of_edges()} edges",
            showlegend=False,
            hovermode='closest',
            margin=dict(b=20, l=5, r=5, t=40),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            plot_bgcolor='white',
            width=800,
            height=600
        )
    )
    
    return fig

async def main():
    conn = None
    
    try:
        print("Starting Fast Network Visualization")
        
        conn = await get_connection()
        relations = await fetch_url_path_relations(conn)
        
        if not relations:
            print("No data found")
            return
        
        print(f"Got {len(relations)} relationships")
        
        # Create fast visualization
        fig = create_fast_network_graph(relations)
        
        if fig is None:
            print("Failed to create graph")
            return
        
        print("Opening visualization...")
        fig.show()
        
        print("Done! Graph should load quickly now.")
        
    except Exception as e:
        print(f"Error: {e}")
        
    finally:
        if conn:
            await return_connection(conn)

if __name__ == "__main__":
    asyncio.run(main())