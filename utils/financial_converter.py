import pandas as pd
import json
import re

class SnapshotConverter:
    def __init__(self, dataframe):
        self.df = dataframe.copy()
        
        # Filling with NaNs 
        self.df['safe_concept'] = self.df['concept'].fillna('unknown_concept')
        self.df['safe_label'] = self.df['label'].fillna('unknown_label')
        self.df['safe_dim'] = self.df['dimension_member'].fillna('NO_DIMENSION')

        # Deduplicate based on the Triplet Key.
        self.df.drop_duplicates(
            subset=['safe_concept', 'safe_label', 'safe_dim'],
            keep='last',
            inplace=True
        )

        # Identify date columns matching YYYY-MM-DD
        self.date_cols = sorted(
            [c for c in self.df.columns if re.match(r'\d{4}-\d{2}-\d{2}', c)],
            reverse=True
        )
        self.tree = []
        self._build_tree()

    def _build_tree(self):
        nodes_by_concept = {}
        all_node_wrappers = [] 

        for _, row in self.df.iterrows():
            # 1. Safe Extraction
            label = row.get('label')
            if pd.isna(label): label = "Unknown Label"
            
            concept = row.get('concept')
            if pd.isna(concept): concept = "unknown_concept"
            
            std_concept = row.get('standard_concept')
            if pd.isna(std_concept): std_concept = None
            
            dim_member = row.get('dimension_member')
            if pd.isna(dim_member): dim_member = None

            # 2. Build Node Structure
            node = {
                "label": label,
                "concept": concept,
                "standard_concept": std_concept, 
                "path": [], 
                "identifiers": {
                    "concept": concept,
                    "dimension_member": dim_member,
                    "label": label
                },
                "data": {},
                "children": []
            }

            # 3. Populate Data
            for date_col in self.date_cols:
                val = row.get(date_col)
                if not pd.isna(val):
                    try:
                        # Attempt to convert to float
                        clean_val = str(val).replace(',', '').strip()
                        float_val = float(clean_val)
                        node['data'][date_col] = float_val
                    except (ValueError, TypeError):
                        # If conversion fails (e.g. a HTML block), simply SKIP adding this data point.
                        pass

            # 4. Register Parents 
            # If a concept appears twice, the 'abstract' one as the definitive parent container is preferred.
            if concept not in nodes_by_concept:
                nodes_by_concept[concept] = node
            elif row.get('abstract') is True:
                # Overwrite if this new row is specifically an abstract header
                nodes_by_concept[concept] = node
            
            all_node_wrappers.append({
                "node": node,
                "parent_concept": row.get('parent_abstract_concept')
            })

        # 5. Link Hierarchy
        roots = []
        for wrapper in all_node_wrappers:
            node = wrapper['node']
            parent_c = wrapper['parent_concept']
            
            # Link to parent if it exists in our lookup
            if parent_c and parent_c in nodes_by_concept:
                nodes_by_concept[parent_c]['children'].append(node)
            else:
                roots.append(node)
        
        self.tree = roots
        self._populate_paths(self.tree, [])

    def _populate_paths(self, nodes, parent_path):
        for node in nodes:
            current_path = parent_path + [node['label']]
            node['path'] = current_path
            self._populate_paths(node['children'], current_path)

    def get_json(self):
        return json.dumps(self.tree, indent=2)