def write_gdoc_letter(
    text_v1: str,
    text_v2: str,
    text_v3: str,
    doc_title: str,
    creds_file: str,
    folder_id: str="1fXbsMrE_MU4wchsll1X4bi9SuK6q5ikV"
    ):
    """
    Creates a formatted cover letter in Google Docs with V1, V2, V3 tabs.
    Each tab gets the same header but different body text.

    Args:
        text_v1: Cover letter text for V1 tab.
        text_v2: Cover letter text for V2 tab.
        text_v3: Cover letter text for V3 tab.
        doc_title: Title for the document.
        creds_file: Path to the credentials file.
        folder_id: The ID of the folder to store the document in.

    Returns:
        Document URL.
    """
    try:
        creds = get_user_credentials()
        service = build('drive', 'v3', credentials=creds)
        
        # Create the Google Docs document
        print("Setting metadata...")
        file_metadata = {
            'name': doc_title,
            'mimeType': 'application/vnd.google-apps.document',
            'parents': [folder_id]
        }
        
        print("Creating document...")
        file = service.files().create(body=file_metadata).execute()
        doc_id = file['id']
        
        # Create the Docs service
        docs_service = build('docs', 'v1', credentials=creds)
        
        # Get the document
        print("Getting document structure...")
        doc = docs_service.documents().get(documentId=doc_id).execute()
        
        # Check if tabs exist, if not use the older structure
        if 'tabs' in doc:
            default_tab_id = doc['tabs'][0]['tabProperties']['tabId']
        else:
            # Fallback: tabs might not be in the response
            # Try fetching with fields parameter
            doc = docs_service.documents().get(
                documentId=doc_id,
                fields='tabs'
            ).execute()
            
            if 'tabs' in doc and len(doc['tabs']) > 0:
                default_tab_id = doc['tabs'][0]['tabProperties']['tabId']
            else:
                # If still no tabs, it might be an older API version
                # In this case, you can omit tabId from locations
                print("Warning: Tabs not found in document structure")
                default_tab_id = None
                
        # Rename first tab to V1 and create V2, V3
        # default_tab_id = doc['tabs'][0]['tabProperties']['tabId']
        
        requests = [
            # Rename first tab to V1
            {
                'updateTabProperties': {
                    'tabId': default_tab_id,
                    'tabProperties': {
                        'title': 'V1'
                    },
                    'fields': 'title'
                }
            },
            # Create V2 tab
            {
                'createTab': {
                    'location': {'index': 1},
                    'tabProperties': {'title': 'V2'}
                }
            },
            # Create V3 tab
            {
                'createTab': {
                    'location': {'index': 2},
                    'tabProperties': {'title': 'V3'}
                }
            }
        ]
        
        print("Creating tabs...")
        docs_service.documents().batchUpdate(
            documentId=doc_id,
            body={'requests': requests}
        ).execute()
        
        # Refresh document to get all tab IDs
        doc = docs_service.documents().get(documentId=doc_id).execute()
        
        # Map tab names to tab IDs and texts
        tab_data = {}
        for i, tab in enumerate(doc['tabs']):
            title = tab['tabProperties']['title']
            tab_id = tab['tabProperties']['tabId']
            
            # Assign the correct text to each tab
            if title == 'V1':
                tab_data[title] = {'id': tab_id, 'text': text_v1}
            elif title == 'V2':
                tab_data[title] = {'id': tab_id, 'text': text_v2}
            elif title == 'V3':
                tab_data[title] = {'id': tab_id, 'text': text_v3}
        
        print(f"Writing to tabs: {list(tab_data.keys())}")
        
        # Define header content (same for all tabs)
        header_text = "Ian A. Davis\niandavis1937@gmail.com              ·            	+1 (518) 278-4071                   ·          	Ballston Spa, NY 12020\n\n"
        
        # Build requests for all three tabs
        all_requests = []
        
        for tab_name, data in tab_data.items():
            tab_id = data['id']
            text = data['text']
            
            print(f"  Preparing content for {tab_name}...")
            
            # Insert header
            all_requests.append({
                'insertText': {
                    'location': {
                        'tabId': tab_id,
                        'index': 1
                    },
                    'text': header_text
                }
            })
            
            # Calculate where body text starts
            current_index = len(header_text) + 1
            
            # Insert body paragraphs
            paragraphs = text.split('\n\n')
            for para in paragraphs:
                if para.strip():
                    all_requests.append({
                        'insertText': {
                            'location': {
                                'tabId': tab_id,
                                'index': current_index
                            },
                            'text': para.strip() + '\n\n'
                        }
                    })
                    current_index += len(para.strip()) + 2
            
            # Format the header and body for this tab
            header_name_end = len("Ian A. Davis") + 1
            header_contact_start = header_name_end + 1
            header_end = len(header_text)
            
            all_requests.extend([
                # Make name bold and 14pt
                {
                    'updateTextStyle': {
                        'range': {
                            'tabId': tab_id,
                            'startIndex': 1,
                            'endIndex': header_name_end
                        },
                        'textStyle': {
                            'bold': True,
                            'fontSize': {'magnitude': 14, 'unit': 'PT'},
                            'weightedFontFamily': {'fontFamily': 'Cambria'}
                        },
                        'fields': 'bold,fontSize,weightedFontFamily'
                    }
                },
                # Left-align name
                {
                    'updateParagraphStyle': {
                        'range': {
                            'tabId': tab_id,
                            'startIndex': 1,
                            'endIndex': header_name_end
                        },
                        'paragraphStyle': {
                            'alignment': 'START'
                        },
                        'fields': 'alignment'
                    }
                },
                # Format contact info (10pt, centered)
                {
                    'updateTextStyle': {
                        'range': {
                            'tabId': tab_id,
                            'startIndex': header_contact_start,
                            'endIndex': header_end
                        },
                        'textStyle': {
                            'fontSize': {'magnitude': 10, 'unit': 'PT'},
                            'weightedFontFamily': {'fontFamily': 'Cambria'}
                        },
                        'fields': 'fontSize,weightedFontFamily'
                    }
                },
                # Center contact info
                {
                    'updateParagraphStyle': {
                        'range': {
                            'tabId': tab_id,
                            'startIndex': header_contact_start,
                            'endIndex': header_end
                        },
                        'paragraphStyle': {
                            'alignment': 'CENTER'
                        },
                        'fields': 'alignment'
                    }
                },
                # Format body text (Times New Roman, 12pt, single-spaced)
                {
                    'updateParagraphStyle': {
                        'range': {
                            'tabId': tab_id,
                            'startIndex': header_end,
                            'endIndex': current_index
                        },
                        'paragraphStyle': {
                            'lineSpacing': 100,
                            'spaceAbove': {'magnitude': 0, 'unit': 'PT'},
                            'spaceBelow': {'magnitude': 0, 'unit': 'PT'},
                            'alignment': 'START'
                        },
                        'fields': 'lineSpacing,spaceAbove,spaceBelow,alignment'
                    }
                },
                {
                    'updateTextStyle': {
                        'range': {
                            'tabId': tab_id,
                            'startIndex': header_end,
                            'endIndex': current_index
                        },
                        'textStyle': {
                            'fontSize': {'magnitude': 12, 'unit': 'PT'},
                            'weightedFontFamily': {'fontFamily': 'Times New Roman'}
                        },
                        'fields': 'fontSize,weightedFontFamily'
                    }
                }
            ])
        
        # Apply all requests at once
        print("Inserting text and formatting for all tabs...")
        docs_service.documents().batchUpdate(
            documentId=doc_id,
            body={'requests': all_requests}
        ).execute()

        doc_url = f'https://docs.google.com/document/d/{doc_id}/edit'
        print(f"✓ Document created with V1, V2, V3 tabs: {doc_url}")
        
        return doc_url
        
    except HttpError as e:
        print(f"✗ Error creating or saving the document: {e}")
        return None