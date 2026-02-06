# streamlit_app/app.py
"""
PE Org-AI-R Platform - Complete Dashboard
CS1: Platform Foundation + CS2: Evidence Collection

Features:
- CS1: Companies, Assessments, Dimension Scores
- CS2: Signal Collection, Patent Analytics, SEC Documents
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from api_client import APIClient
from datetime import datetime
import json

# Page config
st.set_page_config(
    page_title="PE Org-AI-R Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        margin-bottom: 20px;
    }
    .sub-header {
        font-size: 1.8rem;
        font-weight: bold;
        color: #2c3e50;
        margin-top: 30px;
    }
    .metric-card {
        padding: 20px;
        border-radius: 10px;
        background-color: #f0f2f6;
    }
    .score-high {
        color: #28a745;
        font-weight: bold;
        font-size: 1.2rem;
    }
    .score-medium {
        color: #ffc107;
        font-weight: bold;
        font-size: 1.2rem;
    }
    .score-low {
        color: #dc3545;
        font-weight: bold;
        font-size: 1.2rem;
    }
    .status-badge {
        padding: 5px 10px;
        border-radius: 5px;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# Initialize API client
@st.cache_resource
def get_api_client():
    return APIClient()

api = get_api_client()

# Sidebar
st.sidebar.markdown("# 🏢 PE Org-AI-R")
st.sidebar.markdown("**AI-Readiness Assessment Platform**")
st.sidebar.markdown("---")

# Navigation
page = st.sidebar.radio(
    "📍 Navigation",
    [
        "🏠 Dashboard",
        "🏢 Companies",
        "📋 Assessments",
        "📊 Dimension Scores",
        "🎯 Signal Collection",    # CS2
        "📈 Signal Analytics",      # CS2
        "🔬 Patent Deep Dive",      # CS2
        "📄 SEC Documents",         # CS2
        "🔧 System Health"
    ]
)

st.sidebar.markdown("---")
st.sidebar.caption("**Case Study 1:** Platform Foundation ✅")
st.sidebar.caption("**Case Study 2:** Evidence Collection ✅")
st.sidebar.caption("Built with FastAPI + Snowflake + USPTO")

# ============================================
# 🏠 DASHBOARD (Enhanced with CS2)
# ============================================
if page == "🏠 Dashboard":
    st.markdown('<p class="main-header">📊 Platform Dashboard</p>', unsafe_allow_html=True)
    
    try:
        # Get data
        companies = api.list_companies(limit=100)
        assessments = api.list_assessments(limit=100)
        
        # Try to get CS2 signals
        try:
            signals_data = api.get_all_signal_summaries()
            signals_available = True
        except:
            signals_available = False
        
        # Top metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📊 Companies", len(companies))
        
        with col2:
            st.metric("📋 Assessments", len(assessments))
        
        with col3:
            if signals_available:
                st.metric("🎯 Companies with Signals", signals_data.get('count', 0))
            else:
                st.metric("📝 Draft Assessments", sum(1 for a in assessments if a['status'] == 'draft'))
        
        with col4:
            if signals_available and signals_data.get('summaries'):
                avg = sum(s['composite_score'] for s in signals_data['summaries']) / len(signals_data['summaries'])
                st.metric("📈 Avg Composite Score", f"{avg:.1f}/100")
            else:
                st.metric("✅ Approved", sum(1 for a in assessments if a['status'] == 'approved'))
        
        st.markdown("---")
        
        # CS2 Top Performers
        if signals_available and signals_data.get('summaries'):
            st.markdown('<p class="sub-header">🏆 Top Performers (Composite Score)</p>', unsafe_allow_html=True)
            
            top_companies = sorted(
                signals_data['summaries'],
                key=lambda x: x['composite_score'],
                reverse=True
            )[:5]
            
            for i, comp in enumerate(top_companies, 1):
                col1, col2, col3, col4 = st.columns([0.5, 3, 2, 1.5])
                
                with col1:
                    medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"#{i}"
                    st.markdown(f"### {medal}")
                
                with col2:
                    st.markdown(f"**{comp['ticker']}** - {comp['company_name']}")
                
                with col3:
                    # Mini progress bars for each category
                    st.caption(f"Jobs: {comp.get('jobs_score', 0)}/100")
                    st.progress(comp.get('jobs_score', 0) / 100)
                
                with col4:
                    score = comp['composite_score']
                    color_class = "score-high" if score >= 70 else "score-medium" if score >= 40 else "score-low"
                    st.markdown(f'<div class="{color_class}">{score}/100</div>', unsafe_allow_html=True)
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### Companies by Industry")
            if companies:
                try:
                    industries = api.get_industries()
                    industry_map = {str(ind['id']): ind['name'] for ind in industries}
                except:
                    industry_map = {}
                
                industry_counts = {}
                for company in companies:
                    ind_id = str(company['industry_id'])
                    ind_name = industry_map.get(ind_id, ind_id[:8])
                    industry_counts[ind_name] = industry_counts.get(ind_name, 0) + 1
                
                fig = px.bar(
                    x=list(industry_counts.keys()),
                    y=list(industry_counts.values()),
                    labels={'x': 'Industry', 'y': 'Count'}
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("### Assessments by Status")
            if assessments:
                status_counts = {}
                for a in assessments:
                    status_counts[a['status']] = status_counts.get(a['status'], 0) + 1
                
                fig = px.pie(
                    values=list(status_counts.values()),
                    names=list(status_counts.keys())
                )
                st.plotly_chart(fig, use_container_width=True)
    
    except Exception as e:
        st.error(f"Error loading dashboard: {e}")
        st.info("Make sure FastAPI is running: `poetry run uvicorn app.main:create_app --factory --reload`")

# ============================================
# 🏢 COMPANIES (CS1)
# ============================================
elif page == "🏢 Companies":
    st.markdown('<p class="main-header">🏢 Companies Management</p>', unsafe_allow_html=True)
    
    tab1, tab2, tab3 = st.tabs(["📋 List Companies", "➕ Create Company", "✏️ Update Company"])
    
    with tab1:
        st.markdown("### All Companies")
        
        col1, col2 = st.columns([3, 1])
        with col1:
            search = st.text_input("🔍 Search", placeholder="Name or ticker...")
        with col2:
            limit = st.number_input("Per page", 5, 100, 20)
        
        try:
            companies = api.list_companies(limit=limit)
            
            if search:
                companies = [
                    c for c in companies 
                    if search.lower() in c['name'].lower() 
                    or (c.get('ticker') and search.lower() in c['ticker'].lower())
                ]
            
            if companies:
                df = pd.DataFrame(companies)
                df['created_at'] = pd.to_datetime(df['created_at']).dt.strftime('%Y-%m-%d %H:%M')
                
                display_cols = ['name', 'ticker', 'position_factor', 'created_at']
                st.dataframe(df[display_cols], use_container_width=True, hide_index=True)
                
                st.success(f"Showing {len(companies)} companies")
                
                # Delete
                st.markdown("#### 🗑️ Delete Company")
                to_delete = st.selectbox(
                    "Select company",
                    options=[(c['name'], c['id']) for c in companies],
                    format_func=lambda x: x[0]
                )
                
                if st.button("⚠️ Delete", type="secondary"):
                    if st.session_state.get('confirm_delete'):
                        try:
                            api.delete_company(str(to_delete[1]))
                            st.success(f"✅ Deleted {to_delete[0]}")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")
                        st.session_state.confirm_delete = False
                    else:
                        st.session_state.confirm_delete = True
                        st.warning("Click again to confirm!")
            else:
                st.info("No companies found")
        
        except Exception as e:
            st.error(f"Error: {e}")
    
    with tab2:
        st.markdown("### Create New Company")
        
        try:
            industries = api.get_industries()
            industry_opts = {f"{i['name']} ({i['sector']})": i['id'] for i in industries}
            
            with st.form("create_company", clear_on_submit=True):
                col1, col2 = st.columns(2)
                
                with col1:
                    name = st.text_input("Company Name *", placeholder="Apple Inc")
                    industry = st.selectbox("Industry *", list(industry_opts.keys()))
                
                with col2:
                    ticker = st.text_input("Ticker", placeholder="AAPL", max_chars=10)
                    position = st.slider("Position Factor", -1.0, 1.0, 0.0, 0.1)
                
                if st.form_submit_button("✨ Create", use_container_width=True):
                    if name:
                        try:
                            result = api.create_company({
                                "name": name,
                                "ticker": ticker or None,
                                "industry_id": industry_opts[industry],
                                "position_factor": position
                            })
                            st.success(f"✅ Created {name}!")
                            st.balloons()
                        except Exception as e:
                            st.error(f"Error: {e}")
                    else:
                        st.error("Name is required!")
        
        except Exception as e:
            st.error(f"Error: {e}")
    
    with tab3:
        st.markdown("### Update Company")
        
        try:
            companies = api.list_companies(limit=100)
            
            if companies:
                selected = st.selectbox(
                    "Select Company",
                    [(c['name'], c['id']) for c in companies],
                    format_func=lambda x: x[0]
                )
                
                current = api.get_company(str(selected[1]))
                industries = api.get_industries()
                industry_opts = {f"{i['name']} ({i['sector']})": i['id'] for i in industries}
                
                current_ind = None
                for name, iid in industry_opts.items():
                    if str(iid) == str(current['industry_id']):
                        current_ind = name
                        break
                
                with st.form("update_company"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        name = st.text_input("Name", value=current['name'])
                        industry = st.selectbox("Industry", list(industry_opts.keys()), 
                                              index=list(industry_opts.keys()).index(current_ind) if current_ind else 0)
                    
                    with col2:
                        ticker = st.text_input("Ticker", value=current.get('ticker', ''))
                        position = st.slider("Position", -1.0, 1.0, float(current['position_factor']), 0.1)
                    
                    if st.form_submit_button("💾 Update"):
                        try:
                            api.update_company(str(selected[1]), {
                                "name": name,
                                "ticker": ticker or None,
                                "industry_id": industry_opts[industry],
                                "position_factor": position
                            })
                            st.success("✅ Updated!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")
        
        except Exception as e:
            st.error(f"Error: {e}")

# ============================================
# 📋 ASSESSMENTS (CS1)
# ============================================
elif page == "📋 Assessments":
    st.markdown('<p class="main-header">📋 Assessments</p>', unsafe_allow_html=True)
    
    tab1, tab2 = st.tabs(["📋 List", "➕ Create"])
    
    with tab1:
        st.markdown("### All Assessments")
        
        try:
            companies_list = api.list_companies(limit=100)
            
            filter_comp = st.selectbox(
                "Filter by Company",
                [("All", None)] + [(c['name'], c['id']) for c in companies_list],
                format_func=lambda x: x[0]
            )
            
            assessments = api.list_assessments(
                limit=50,
                company_id=str(filter_comp[1]) if filter_comp[1] else None
            )
            
            if assessments:
                for a in assessments:
                    try:
                        comp = api.get_company(str(a['company_id']))
                        comp_name = comp['name']
                    except:
                        comp_name = str(a['company_id'])[:8]
                    
                    status_emoji = {
                        'draft': '🟡', 'in_progress': '🔵',
                        'submitted': '🟣', 'approved': '🟢'
                    }.get(a['status'], '⚪')
                    
                    with st.expander(f"{status_emoji} {comp_name} - {a['assessment_type']}"):
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.write(f"**Type:** {a['assessment_type']}")
                            st.write(f"**Status:** {a['status']}")
                        
                        with col2:
                            st.write(f"**Primary:** {a.get('primary_assessor', 'N/A')}")
                            st.write(f"**Secondary:** {a.get('secondary_assessor', 'N/A')}")
                        
                        with col3:
                            vr = a.get('vr_score')
                            if vr:
                                st.metric("VR Score", f"{vr:.1f}")
                            st.caption(f"Created: {a['created_at'][:10]}")
                        
                        # Update status
                        new_status = st.selectbox(
                            "Update Status",
                            ['draft', 'in_progress', 'submitted', 'approved'],
                            index=['draft', 'in_progress', 'submitted', 'approved'].index(a['status']),
                            key=f"s_{a['id']}"
                        )
                        
                        if st.button("Update", key=f"b_{a['id']}"):
                            if new_status != a['status']:
                                try:
                                    api.update_assessment_status(str(a['id']), new_status)
                                    st.success(f"✅ Updated to {new_status}!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")
        
        except Exception as e:
            st.error(f"Error: {e}")
    
    with tab2:
        st.markdown("### Create Assessment")
        
        try:
            companies_list = api.list_companies(limit=100)
            
            if not companies_list:
                st.warning("Create companies first!")
            else:
                comp_opts = {f"{c['name']} ({c.get('ticker', 'N/A')})": c['id'] for c in companies_list}
                
                with st.form("create_assess", clear_on_submit=True):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        comp = st.selectbox("Company *", list(comp_opts.keys()))
                        type_ = st.selectbox("Type *", ['screening', 'due_diligence', 'quarterly', 'exit_prep'],
                                           format_func=lambda x: x.replace('_', ' ').title())
                    
                    with col2:
                        primary = st.text_input("Primary Assessor *")
                        secondary = st.text_input("Secondary Assessor")
                    
                    if st.form_submit_button("✨ Create", use_container_width=True):
                        if primary:
                            try:
                                result = api.create_assessment({
                                    "company_id": comp_opts[comp],
                                    "assessment_type": type_,
                                    "primary_assessor": primary,
                                    "secondary_assessor": secondary or None
                                })
                                st.success("✅ Assessment created!")
                                st.info(f"ID: {result['id']}")
                                st.balloons()
                            except Exception as e:
                                st.error(f"Error: {e}")
                        else:
                            st.error("Primary assessor required!")
        
        except Exception as e:
            st.error(f"Error: {e}")

# ============================================
# 📊 DIMENSION SCORES (CS1)
# ============================================
elif page == "📊 Dimension Scores":
    st.markdown('<p class="main-header">📊 Dimension Scores</p>', unsafe_allow_html=True)
    
    try:
        assessments = api.list_assessments(limit=100)
        
        if not assessments:
            st.warning("Create assessments first!")
        else:
            # Select assessment
            assess_opts = []
            for a in assessments:
                try:
                    comp = api.get_company(str(a['company_id']))
                    label = f"{comp['name']} - {a['assessment_type']} ({a['status']})"
                except:
                    label = f"{a['assessment_type']} ({a['status']})"
                assess_opts.append((label, a['id']))
            
            selected = st.selectbox("Select Assessment", assess_opts, format_func=lambda x: x[0])
            assess_id = str(selected[1])
            
            tab1, tab2 = st.tabs(["📈 View Scores", "➕ Add Score"])
            
            with tab1:
                try:
                    scores = api.get_dimension_scores(assess_id)
                    
                    if scores:
                        # Metrics
                        total_weighted = sum(s['score'] * s['weight'] for s in scores)
                        
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Dimensions", len(scores))
                        with col2:
                            avg = sum(s['score'] for s in scores) / len(scores)
                            st.metric("Average", f"{avg:.1f}")
                        with col3:
                            st.metric("**Weighted Score**", f"{total_weighted:.1f}/100")
                        
                        # Table
                        df = pd.DataFrame(scores)
                        df['dimension'] = df['dimension'].str.replace('_', ' ').str.title()
                        df['weight'] = (df['weight'] * 100).round(0).astype(int).astype(str) + '%'
                        
                        st.dataframe(
                            df[['dimension', 'score', 'weight', 'confidence', 'evidence_count']],
                            use_container_width=True,
                            hide_index=True
                        )
                        
                        # Radar chart
                        fig = go.Figure()
                        fig.add_trace(go.Scatterpolar(
                            r=[s['score'] for s in scores],
                            theta=[s['dimension'].replace('_', ' ').title() for s in scores],
                            fill='toself'
                        ))
                        fig.update_layout(
                            polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
                            height=500
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No scores yet!")
                
                except:
                    st.info("No scores yet!")
            
            with tab2:
                st.markdown("### Add Score")
                
                try:
                    existing = api.get_dimension_scores(assess_id)
                    existing_dims = [s['dimension'] for s in existing]
                except:
                    existing_dims = []
                
                all_dims = [
                    ("data_infrastructure", "Data Infrastructure", 0.25),
                    ("ai_governance", "AI Governance", 0.20),
                    ("technology_stack", "Technology Stack", 0.15),
                    ("talent_skills", "Talent & Skills", 0.15),
                    ("leadership_vision", "Leadership & Vision", 0.10),
                    ("use_case_portfolio", "Use Case Portfolio", 0.10),
                    ("culture_change", "Culture & Change", 0.05),
                ]
                
                available = [d for d in all_dims if d[0] not in existing_dims]
                
                if not available:
                    st.success("✅ All 7 dimensions scored!")
                else:
                    with st.form("add_score", clear_on_submit=True):
                        dim = st.selectbox(
                            "Dimension",
                            available,
                            format_func=lambda x: f"{x[1]} (Weight: {int(x[2]*100)}%)"
                        )
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            score = st.slider("Score", 0, 100, 75)
                            conf = st.slider("Confidence", 0.0, 1.0, 0.8, 0.05)
                        with col2:
                            evidence = st.number_input("Evidence Count", 0, value=5)
                        
                        if st.form_submit_button("✨ Add", use_container_width=True):
                            try:
                                api.create_dimension_score(assess_id, {
                                    "assessment_id": assess_id,
                                    "dimension": dim[0],
                                    "score": score,
                                    "confidence": conf,
                                    "evidence_count": evidence
                                })
                                st.success(f"✅ {dim[1]} score added!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error: {e}")
    
    except Exception as e:
        st.error(f"Error: {e}")

# ============================================
# 🎯 SIGNAL COLLECTION (CS2 - NEW!)
# ============================================
elif page == "🎯 Signal Collection":
    st.markdown('<p class="main-header">🎯 External Signal Collection</p>', unsafe_allow_html=True)
    
    st.markdown("""
    Collect external signals to measure the **Say-Do Gap**:
    - 🔵 **Jobs** (30%) - AI/ML hiring activity  
    - 🟢 **Tech** (25%) - Technology stack maturity
    - 🟣 **Patents** (25%) - Innovation activity
    - 🟠 **Leadership** (20%) - Executive AI expertise
    """)
    
    st.markdown("---")
    
    try:
        companies = api.list_companies(limit=100)
        
        if not companies:
            st.warning("Add companies first!")
        else:
            col1, col2 = st.columns([2, 1])
            
            with col1:
                comp_choice = st.selectbox(
                    "Select Company",
                    [(f"{c['ticker']} - {c['name']}", c['ticker']) for c in companies if c.get('ticker')],
                    format_func=lambda x: x[0]
                )
                ticker = comp_choice[1]
            
            with col2:
                years = st.number_input("Years (Patents)", 1, 10, 5)
            
            location = st.text_input("Job Location", "United States")
            
            st.markdown("---")
            
            # Collection buttons
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("🚀 Collect ALL Signals", type="primary", use_container_width=True):
                    with st.spinner(f"Collecting signals for {ticker}..."):
                        try:
                            result = api.collect_all_signals(ticker, years, location)
                            st.success(f"✅ Collection started for {ticker}!")
                            st.info("Takes 30-60 seconds. Check Signal Analytics for results.")
                            with st.expander("Response"):
                                st.json(result)
                        except Exception as e:
                            st.error(f"Error: {e}")
            
            with col2:
                if st.button("🔬 Patents Only", use_container_width=True):
                    with st.spinner("Collecting patents..."):
                        try:
                            result = api.collect_patents_only(ticker, years)
                            st.success("✅ Patent collection started!")
                            with st.expander("Response"):
                                st.json(result)
                        except Exception as e:
                            st.error(f"Error: {e}")
            
            with col3:
                if st.button("🔄 Refresh", use_container_width=True):
                    st.rerun()
            
            # Current status
            st.markdown("---")
            st.markdown("### Current Status")
            
            try:
                summary = api.get_signal_summary(ticker)
                
                col1, col2, col3, col4, col5 = st.columns(5)
                
                with col1:
                    st.metric("Jobs", f"{summary.get('jobs_score', 0)}/100")
                with col2:
                    st.metric("Tech", f"{summary.get('tech_score', 0)}/100")
                with col3:
                    st.metric("Patents", f"{summary.get('patents_score', 0)}/100")
                with col4:
                    st.metric("Leadership", f"{summary.get('leadership_score', 0)}/100")
                with col5:
                    st.metric("**Composite**", f"{summary.get('composite_score', 0)}/100")
                
                st.caption(f"Last updated: {summary.get('last_updated', 'Never')}")
            
            except:
                st.info(f"No signals yet for {ticker}. Click 'Collect ALL Signals' above!")
    
    except Exception as e:
        st.error(f"Error: {e}")

# ============================================
# 📈 SIGNAL ANALYTICS (CS2 - NEW!)
# ============================================
elif page == "📈 Signal Analytics":
    st.markdown('<p class="main-header">📈 Signal Analytics</p>', unsafe_allow_html=True)
    
    try:
        data = api.get_all_signal_summaries()
        summaries = data.get('summaries', [])
        
        if not summaries:
            st.warning("No signal data! Go to Signal Collection first.")
        else:
            # Metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Companies Analyzed", len(summaries))
            with col2:
                avg = sum(s['composite_score'] for s in summaries) / len(summaries)
                st.metric("Avg Composite", f"{avg:.1f}/100")
            with col3:
                total = sum(s.get('signal_count', 0) for s in summaries)
                st.metric("Total Signals", total)
            with col4:
                top = max(summaries, key=lambda x: x['composite_score'])
                st.metric("Top Performer", f"{top['ticker']} ({top['composite_score']})")
            
            st.markdown("---")
            
            # Comparison table
            st.markdown("### Company Comparison")
            
            df = pd.DataFrame(summaries)
            df = df.rename(columns={
                'ticker': 'Ticker',
                'company_name': 'Company',
                'jobs_score': 'Jobs',
                'patents_score': 'Patents',
                'tech_score': 'Tech',
                'leadership_score': 'Leadership',
                'composite_score': 'Composite'
            })
            
            display = df[['Ticker', 'Company', 'Jobs', 'Patents', 'Tech', 'Leadership', 'Composite']].copy()
            
            # Color coding
            def color_score(val):
                if pd.isna(val):
                    return ''
                color = '#28a745' if val >= 70 else '#ffc107' if val >= 40 else '#dc3545'
                return f'background-color: {color}; color: white;'
            
            styled = display.style.applymap(
                color_score,
                subset=['Jobs', 'Patents', 'Tech', 'Leadership', 'Composite']
            )
            
            st.dataframe(styled, use_container_width=True, hide_index=True)
            
            st.markdown("---")
            
            # Visualizations
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### Signal Breakdown")
                
                fig = go.Figure()
                fig.add_trace(go.Bar(name='Jobs (30%)', x=df['Ticker'], y=df['Jobs'], marker_color='#3498db'))
                fig.add_trace(go.Bar(name='Patents (25%)', x=df['Ticker'], y=df['Patents'], marker_color='#9b59b6'))
                fig.add_trace(go.Bar(name='Tech (25%)', x=df['Ticker'], y=df['Tech'], marker_color='#2ecc71'))
                fig.add_trace(go.Bar(name='Leadership (20%)', x=df['Ticker'], y=df['Leadership'], marker_color='#e74c3c'))
                
                fig.update_layout(barmode='group', yaxis_title="Score")
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.markdown("#### Composite Distribution")
                
                fig = px.scatter(
                    df, x='Patents', y='Jobs', size='Composite', color='Composite',
                    hover_data=['Ticker', 'Company'],
                    color_continuous_scale='RdYlGn',
                    labels={'Patents': 'Innovation', 'Jobs': 'Hiring'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Say-Do Gap
            st.markdown("---")
            st.markdown("### 🎯 Say-Do Gap Analysis")
            
            df['Gap'] = abs(df['Patents'] - df['Jobs'])
            top_gaps = df.nlargest(8, 'Gap')
            
            fig = px.bar(
                top_gaps, x='Ticker', y='Gap',
                color='Gap', color_continuous_scale='Reds',
                title="Largest Say-Do Gaps"
            )
            st.plotly_chart(fig, use_container_width=True)
            
            st.info("💡 **High gap** = Companies innovate (patents) but don't hire (jobs). Might be outsourcing or overstating AI.")
    
    except Exception as e:
        st.error(f"Error: {e}")

# ============================================
# 🔬 PATENT DEEP DIVE (CS2 - NEW!)
# ============================================
elif page == "🔬 Patent Deep Dive":
    st.markdown('<p class="main-header">🔬 Patent Deep Dive</p>', unsafe_allow_html=True)
    
    try:
        data = api.get_all_signal_summaries()
        summaries = data.get('summaries', [])
        
        if not summaries:
            st.warning("Collect signals first!")
        else:
            # Company selector
            comp_choice = st.selectbox(
                "Select Company",
                [(f"{s['ticker']} - {s['company_name']} (Score: {s['patents_score']}/100)", s['ticker']) for s in summaries],
                format_func=lambda x: x[0]
            )
            ticker = comp_choice[1]
            
            try:
                signals = api.get_signals_by_ticker(ticker)
                patent_sigs = [s for s in signals.get('signals', []) 
                              if 'innovation' in s.get('category', '').lower() or 'patent' in s.get('category', '').lower()]
                
                if patent_sigs:
                    sig = patent_sigs[0]
                    meta = json.loads(sig.get('metadata', '{}')) if isinstance(sig.get('metadata'), str) else sig.get('metadata', {})
                    
                    # Key metrics
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("Total Patents", meta.get('total_patents', 0))
                    with col2:
                        st.metric("AI Patents", meta.get('ai_patents', 0))
                    with col3:
                        st.metric("Recent (1yr)", meta.get('recent_ai_patents', 0))
                    with col4:
                        st.metric("Categories", meta.get('category_count', 0))
                    
                    st.markdown("---")
                    
                    # Score breakdown
                    st.markdown("### Score Breakdown")
                    
                    breakdown = meta.get('score_breakdown', {})
                    
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("Count", f"{breakdown.get('patent_count', 0)}/50")
                        st.caption("5 pts/patent (max 50)")
                    with col2:
                        st.metric("Recency", f"{breakdown.get('recency', 0)}/20")
                        st.caption("2 pts/recent (max 20)")
                    with col3:
                        st.metric("Diversity", f"{breakdown.get('diversity', 0)}/30")
                        st.caption("10 pts/category (max 30)")
                    with col4:
                        total = sum(breakdown.values())
                        st.metric("**Total**", f"{total}/100")
                        st.caption(f"{meta.get('maturity_level', 'Unknown')}")
                    
                    # Categories
                    st.markdown("### Categories")
                    categories = meta.get('categories', [])
                    if categories:
                        category_names = {
                            'ml_core': '🤖 ML Core',
                            'nlp': '💬 NLP',
                            'computer_vision': '👁️ Computer Vision',
                            'predictive': '📊 Predictive',
                            'automation': '🤖 Automation'
                        }
                        cols = st.columns(len(categories))
                        for i, cat in enumerate(categories):
                            with cols[i]:
                                st.info(category_names.get(cat, cat))
                    
                    # Sample patents
                    st.markdown("### Sample Patents")
                    samples = meta.get('sample_patents', [])
                    if samples:
                        for p in samples[:5]:
                            with st.expander(f"📄 {p.get('number', 'N/A')}"):
                                st.markdown(f"**Title:** {p.get('title', 'N/A')}")
                                st.markdown(f"**Categories:** {', '.join(p.get('categories', []))}")
                                st.markdown(f"**CPC:** {', '.join(p.get('cpc_codes', []))}")
                else:
                    st.warning(f"No patent signals for {ticker}")
            
            except Exception as e:
                st.error(f"Error: {e}")
    
    except Exception as e:
        st.error(f"Error: {e}")

# ============================================
# 📄 SEC DOCUMENTS (CS2 - NEW!)
# ============================================
elif page == "📄 SEC Documents":
    st.markdown('<p class="main-header">📄 SEC Documents</p>', unsafe_allow_html=True)
    
    tab1, tab2 = st.tabs(["📥 Collect Documents", "📋 View Documents"])
    
    with tab1:
        st.markdown("### Collect SEC Filings")
        
        try:
            companies = api.list_companies(limit=100)
            
            if companies:
                comp_choice = st.selectbox(
                    "Select Company",
                    [(f"{c['ticker']} - {c['name']}", c['ticker']) for c in companies if c.get('ticker')],
                    format_func=lambda x: x[0]
                )
                ticker = comp_choice[1]
                
                col1, col2 = st.columns(2)
                
                with col1:
                    filing_types = st.multiselect(
                        "Filing Types",
                        ["10-K", "10-Q", "8-K", "DEF 14A"],
                        default=["10-K", "10-Q"]
                    )
                
                with col2:
                    limit = st.number_input("Limit per type", 1, 5, 1)
                
                steps = st.multiselect(
                    "Pipeline Steps",
                    ["download", "parse", "clean", "chunk"],
                    default=["download", "parse", "clean", "chunk"]
                )
                
                if st.button("📥 Collect Documents", type="primary", use_container_width=True):
                    if filing_types and steps:
                        with st.spinner(f"Collecting documents for {ticker}..."):
                            try:
                                result = api.collect_documents(ticker, filing_types, limit, steps)
                                st.success(f"✅ Collection complete!")
                                st.json(result)
                            except Exception as e:
                                st.error(f"Error: {e}")
                    else:
                        st.error("Select at least one filing type and step!")
        
        except Exception as e:
            st.error(f"Error: {e}")
    
    with tab2:
        st.markdown("### View SEC Documents")
        
        try:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                ticker_filter = st.text_input("Filter by Ticker", placeholder="WMT")
            with col2:
                filing_filter = st.selectbox("Filing Type", ["All", "10-K", "10-Q", "8-K", "DEF 14A"])
            with col3:
                status_filter = st.selectbox("Status", ["All", "downloaded", "parsed", "cleaned", "chunked"])
            
            docs_data = api.list_documents(
                ticker=ticker_filter if ticker_filter else None,
                filing_type=filing_filter if filing_filter != "All" else None,
                status=status_filter if status_filter != "All" else None,
                limit=50
            )
            
            docs = docs_data.get('items', [])
            
            if docs:
                st.success(f"Found {len(docs)} documents")
                
                for doc in docs:
                    status_color = {
                        'downloaded': '🟡',
                        'parsed': '🔵',
                        'cleaned': '🟣',
                        'chunked': '🟢',
                        'failed': '🔴'
                    }.get(doc.get('status', ''), '⚪')
                    
                    with st.expander(f"{status_color} {doc.get('ticker')} - {doc.get('filing_type')} ({doc.get('status')})"):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.write(f"**Filing Date:** {doc.get('filing_date', 'N/A')}")
                            st.write(f"**Status:** {doc.get('status')}")
                            st.write(f"**Chunks:** {doc.get('chunk_count', 0)}")
                        
                        with col2:
                            st.write(f"**S3 Key:** `{doc.get('s3_key', 'N/A')[:50]}...`")
                            if doc.get('source_url'):
                                st.markdown(f"[View on SEC.gov]({doc['source_url']})")
                        
                        # View chunks
                        if doc.get('chunk_count', 0) > 0:
                            if st.button("View Chunks", key=f"chunk_{doc['id']}"):
                                try:
                                    chunks_data = api.get_document_chunks(doc['id'], limit=10)
                                    chunks = chunks_data.get('items', [])
                                    
                                    st.markdown(f"**Showing {len(chunks)} chunks:**")
                                    for chunk in chunks:
                                        st.text_area(
                                            f"Chunk {chunk.get('chunk_index', 'N/A')}",
                                            chunk.get('content', '')[:500],
                                            height=150,
                                            key=f"chunk_content_{chunk['id']}"
                                        )
                                except Exception as e:
                                    st.error(f"Error loading chunks: {e}")
            else:
                st.info("No documents found. Collect some in the 'Collect Documents' tab!")
        
        except Exception as e:
            st.error(f"Error: {e}")

# ============================================
# 🔧 SYSTEM HEALTH
# ============================================
elif page == "🔧 System Health":
    st.markdown('<p class="main-header">🔧 System Health</p>', unsafe_allow_html=True)
    
    try:
        health = api.get_health()
        
        if health['status'] == 'healthy':
            st.success(f"## ✅ System: {health['status'].upper()}")
        else:
            st.warning(f"## ⚠️ System: {health['status'].upper()}")
        
        st.markdown("---")
        
        # Dependencies
        st.markdown("### Dependencies")
        
        deps = health.get('dependencies', {})
        cols = st.columns(len(deps))
        
        for i, (service, status) in enumerate(deps.items()):
            with cols[i]:
                if status == 'healthy':
                    st.success(f"✅ {service.capitalize()}")
                elif status == 'not_configured':
                    st.info(f"ℹ️ {service.capitalize()}")
                else:
                    st.error(f"❌ {service.capitalize()}")
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        with col1:
            st.info(f"**Version:** {health.get('version', 'Unknown')}")
        with col2:
            st.info(f"**Checked:** {health.get('timestamp', 'Unknown')}")
        
        with st.expander("Full Response"):
            st.json(health)
    
    except Exception as e:
        st.error("❌ Cannot connect to API")
        st.error(f"Error: {e}")
        st.info("**Troubleshooting:**")
        st.code("poetry run uvicorn app.main:create_app --factory --reload")

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("### 🎓 Team Information")
st.sidebar.caption("BigDataIA - Spring 2026")
st.sidebar.caption("Team 03")
st.sidebar.caption(f"App Version: 2.0")