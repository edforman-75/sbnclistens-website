#!/usr/bin/env python3
"""
Aggregate Jobs-to-be-Done (JTBD) data from all analysis files.
Filters for website-related jobs only (in-scope).
"""

import os
import json
from collections import defaultdict

# Scope filtering keywords - same as used in other aggregations
WEBSITE_KEYWORDS = [
    'website', 'web site', 'site', 'app', 'application', 'online', 'digital',
    'interface', 'navigation', 'navigate', 'menu', 'page', 'click', 'button',
    'link', 'search', 'filter', 'browse', 'scroll', 'calendar', 'list view',
    'register', 'sign up', 'login', 'log in', 'profile', 'update information',
    'wild apricot', 'platform', 'system', 'technology', 'tech', 'ux', 'ui',
    'user experience', 'user interface', 'design', 'layout', 'visual',
    'notification', 'email', 'message', 'communication online', 'rsvp',
    'event listing', 'event page', 'committee page', 'member directory'
]

def is_in_scope(text):
    """Check if text contains website-related keywords."""
    if not text:
        return False
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in WEBSITE_KEYWORDS)

def is_job_in_scope(job):
    """Check if a job is related to the website."""
    # Check all text fields in the job
    fields_to_check = [
        job.get('job_statement', ''),
        job.get('current_solution', ''),
        job.get('pain_points', ''),
        job.get('desired_outcomes', ''),
        job.get('success_criteria', '')
    ]

    return any(is_in_scope(field) for field in fields_to_check)

def aggregate_jtbd():
    """Aggregate all JTBD files with scope filtering."""

    jtbd_dir = "analysis/jtbd"

    # Storage for aggregated jobs by category
    aggregated = {
        'functional_jobs': [],
        'emotional_jobs': [],
        'social_jobs': [],
        'supporting_jobs': []
    }

    # Statistics
    stats = {
        'total_files': 0,
        'total_jobs_raw': 0,
        'total_jobs_in_scope': 0,
        'by_category_raw': defaultdict(int),
        'by_category_in_scope': defaultdict(int)
    }

    # Read all JTBD files
    for filename in os.listdir(jtbd_dir):
        if not filename.endswith('_jtbd.json'):
            continue

        stats['total_files'] += 1
        filepath = os.path.join(jtbd_dir, filename)

        try:
            with open(filepath, 'r') as f:
                data = json.load(f)

            jobs_data = data.get('jobs', {})
            source_info = data.get('_source', {})
            interview_id = source_info.get('interview_id', filename.replace('_jtbd.json', ''))

            # Process each category
            for category in ['functional_jobs', 'emotional_jobs', 'social_jobs', 'supporting_jobs']:
                jobs_list = jobs_data.get(category, [])

                for job in jobs_list:
                    stats['total_jobs_raw'] += 1
                    stats['by_category_raw'][category] += 1

                    # Check if job is in scope (website-related)
                    if is_job_in_scope(job):
                        stats['total_jobs_in_scope'] += 1
                        stats['by_category_in_scope'][category] += 1

                        # Add source information to the job
                        job_with_source = job.copy()
                        job_with_source['_source'] = {
                            'interview_id': interview_id,
                            'file': filename
                        }

                        aggregated[category].append(job_with_source)

        except Exception as e:
            print(f"Error processing {filename}: {e}")
            continue

    # Sort jobs by frequency of similar statements (simple grouping by job_statement similarity)
    # For now, just keep them in order - could add more sophisticated clustering later

    # Create output structure
    output = {
        'jobs_to_be_done': {
            'functional': {
                'total': len(aggregated['functional_jobs']),
                'jobs': aggregated['functional_jobs']
            },
            'emotional': {
                'total': len(aggregated['emotional_jobs']),
                'jobs': aggregated['emotional_jobs']
            },
            'social': {
                'total': len(aggregated['social_jobs']),
                'jobs': aggregated['social_jobs']
            },
            'supporting': {
                'total': len(aggregated['supporting_jobs']),
                'jobs': aggregated['supporting_jobs']
            }
        },
        '_metadata': {
            'scope': 'website_only',
            'total_interviews_analyzed': stats['total_files'],
            'total_jobs_identified': stats['total_jobs_raw'],
            'total_jobs_in_scope': stats['total_jobs_in_scope'],
            'filtering_applied': True,
            'statistics': {
                'raw_counts': dict(stats['by_category_raw']),
                'in_scope_counts': dict(stats['by_category_in_scope'])
            }
        }
    }

    # Save aggregated JTBD
    output_file = "analysis/AGGREGATED_JTBD.json"
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"✓ JTBD Aggregation Complete")
    print(f"  Files processed: {stats['total_files']}")
    print(f"  Total jobs found: {stats['total_jobs_raw']}")
    print(f"  Website-related jobs: {stats['total_jobs_in_scope']}")
    print(f"  → Functional: {len(aggregated['functional_jobs'])}")
    print(f"  → Emotional: {len(aggregated['emotional_jobs'])}")
    print(f"  → Social: {len(aggregated['social_jobs'])}")
    print(f"  → Supporting: {len(aggregated['supporting_jobs'])}")
    print(f"  Saved to: {output_file}")

    return output

if __name__ == "__main__":
    aggregate_jtbd()
