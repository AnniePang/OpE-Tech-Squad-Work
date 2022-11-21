#!/usr/bin/env python
# coding: utf-8

# In[2]:


import random 


# In[6]:


import pandas as pd 

nodes_df = pd.read_excel('IN_Supply_Chain_Model.xlsx', sheet_name = 0)
edges_df = pd.read_excel('IN_Supply_Chain_Model.xlsx', sheet_name = 1) 


# In[9]:


node_storage_lookup = nodes_df["Supply"].copy()
print(node_storage_lookup)


# In[11]:


all_edges = edges_df[["Edge ID", "Start Node", "End Node", "Capacity"]]
all_edges.head()


# In[18]:


import pickle 

with open("top-sorted-nodes", "rb") as file:
    top_sort_nodes = pickle.load(file)
    
print(top_sort_nodes)


# In[19]:


# Internal function used for flow object generation 
def update_df(output_df, start_node, end_node, amount): 
    cond = ((output_df['source'] == start_node) & (output_df['sink'] == end_node)).any()

    if cond: 
        output_df['amount'].where((output_df['source'] != start_node) | (output_df['sink'] != end_node), 
                                                        other=output_df['amount']+amount, inplace=True) 
    else: 
        output_df.loc[len(output_df.index)] = [start_node, end_node, amount] 

# all_edges: all the edge mappings between nodes in the graph, lookup: amount of fuel in each node, 
# top_sort_nodes: order by which nodes should be traversed (based on a topological sorting algorithm) 
def propagate(all_edges, lookup, top_sort_nodes):
    
    # Contains the amount of fuel remaining in each node 
    result_lookup = lookup.copy() 
    
    output_df = pd.DataFrame({'source': [], 'sink': [], 'amount': []})
    output_df.index.rename('index', inplace=True)
    
    # Go through the nodes in topological sort order 
    for start_node in top_sort_nodes:
        
        # Set index and extract all edges that are connected to the current start_node 
        curr_index = 0 
        
        curr_edges = all_edges[all_edges['Start Node']==start_node]
        curr_edges = curr_edges.reset_index().drop(columns='index')
        curr_edges.index.rename('index', inplace=True)
        
        # TODO: Implement storage functionality  
        
        # Iterate through all the edges connected with start_node 
        while curr_index < len(curr_edges):
        
            # Extract end node ID and capacity of edge 
            end_node, edge_capacity = curr_edges.at[curr_index, 'End Node'], curr_edges.at[curr_index, 'Capacity']
        
            # Special case accounting for whether this is the last edge we are iterating through 
            if curr_index==len(curr_edges)-1:
                
                # If the amount of fuel remaining in the start node is less than or equal to the edge capacity, 
                # propagate all the remaining fuel down the last edge 
                if result_lookup[start_node-1]<= edge_capacity: 
                    propagate_amount = result_lookup[start_node-1]
                # Otherwise, generate a random amount between 0 and the edge capacity to propagate down the graph
                else: 
                    propagate_amount = random.randint(0, edge_capacity)
                
                # Update the result_lookup, which contains the amount of fuel remaining in each node 
                update_df(output_df, start_node, end_node, propagate_amount) #print(start_node, end_node, propagate_amount)
                result_lookup[start_node-1] -= propagate_amount 
                result_lookup[end_node-1] += propagate_amount 
                curr_edges.at[curr_index,'Capacity'] -= propagate_amount 
                
                # Remove the edge from our current data frame iteration if its capacity is reached 
                if curr_edges.at[curr_index,'Capacity']==0:
                    curr_edges = curr_edges.drop(curr_index)
                    curr_edges = curr_edges.reset_index().drop(columns='index')
                    curr_edges.index.rename('index', inplace=True)
            
                # Update iteration index: If we still have fuel remaining and the edges have not reached their capacities, 
                # continue propagation. Otherwise, break out of the while loop and move on to the next start node 
                if result_lookup[start_node-1] == 0 or len(curr_edges)==0: 
                    break
                else: 
                    curr_index = 0 
            
            # When we are iterating through all edges but the last edge  
            else: 
                # Generate a random fuel amount to propagate down the graph taking into consideration flow constraints
                # and update the result_lookup, which contains the amount of fuel remaining in each node 
                propagate_amount = random.randint(0, min(edge_capacity, result_lookup[start_node-1]))
                update_df(output_df, start_node, end_node, propagate_amount) #print(start_node, end_node, propagate_amount)
                result_lookup[start_node-1] -= propagate_amount 
                result_lookup[end_node-1] += propagate_amount 
                curr_edges.at[curr_index,'Capacity'] -= propagate_amount 
                
                # Remove the edge from our current data frame iteration if its capacity is reached 
                if curr_edges.at[curr_index,'Capacity']==0:
                    curr_edges = curr_edges.drop(curr_index)
                    curr_edges = curr_edges.reset_index().drop(columns='index')
                    curr_edges.index.rename('index', inplace=True)
                    curr_index -= 1
                
                # Update iteration index 
                curr_index += 1
                
    
    # generate the flow objects for each edge 
    flow = list(output_df.to_dict('index').values()) 
    
    # return the flow and result_lookup          
    return flow, result_lookup


# In[20]:


flow, result = propagate(all_edges.copy(), node_storage_lookup.copy(), top_sort_nodes)


# In[21]:


print(result)


# In[22]:


for x in flow:
    print(x)


# In[23]:


flow, result = propagate(all_edges.copy(), node_storage_lookup.copy(), top_sort_nodes)


# In[24]:


print(result)


# In[25]:


for x in flow:
    print(x)


# In[28]:


flow, result = propagate(all_edges.copy(), node_storage_lookup.copy(), top_sort_nodes)


# In[29]:


print(result)


# In[ ]:




