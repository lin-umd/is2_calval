#!/usr/bin/env python
# coding: utf-8

import glob
import os
from PIL import Image, ImageDraw, ImageFont
from reportlab.pdfgen import canvas

VALID_SITES = ['amani','csir_agincourt', 'csir_dnyala', 'csir_ireagh', 'csir_justicia', 'csir_venetia', 'csir_welverdient', 'drc_ghent_field_32635', 
               'drc_ghent_field_32733', 'drc_ghent_field_32734', 'gsfc_mozambique', 'jpl_lope', 'jpl_rabi', 'tanzania_wwf_germany', 'khaoyai_thailand', 
               'chowilla', 'credo', 'karawatha', 'litchfield', 'rushworth_forests', 'tern_alice_mulga', 'tern_robson_whole', 'costarica_laselva2019', 
               'skidmore_bayerischer', 'zofin_180607', 'spain_exts1', 'spain_exts2', 'spain_exts3', 'spain_exts4', 'spain_leonposada', 'spain_leon1', 
               'spain_leon2', 'spain_leon3', 'jpl_borneo_004', 'jpl_borneo_013', 'jpl_borneo_040', 'jpl_borneo_119', 'jpl_borneo_144', 'chave_paracou', 
               'embrapa_brazil_2020_and_a01', 'embrapa_brazil_2020_bon_a01', 'embrapa_brazil_2020_cau_a01', 'embrapa_brazil_2020_duc_a01', 
               'embrapa_brazil_2020_hum_a01', 'embrapa_brazil_2020_par_a01', 'embrapa_brazil_2020_rib_a01', 'embrapa_brazil_2020_tal_a01',
               'embrapa_brazil_2020_tan_a01', 'embrapa_brazil_2020_tap_a01', 'embrapa_brazil_2020_tap_a04', 'walkerfire_20191007', 
               'neon_abby2018', 'neon_abby2019', 'neon_abby2021', 'neon_bart2018', 'neon_bart2019', 'neon_blan2019', 'neon_blan2021', 
               'neon_clbj2018', 'neon_clbj2019', 'neon_clbj2021', 'neon_clbj2021', 'neon_dela2018', 'neon_dela2019', 'neon_dela2021', 
               'neon_dsny2018', 'neon_dsny2021', 'neon_grsm2018', 'neon_grsm2021', 'neon_guan2018', 'neon_harv2018', 'neon_harv2019', 
               'neon_jerc2019', 'neon_jerc2021', 'neon_jorn2018', 'neon_jorn2019', 'neon_jorn2021', 'neon_konz2019', 'neon_konz2020', 
               'neon_leno2018', 'neon_leno2019', 'neon_leno2021', 'neon_mlbs2018', 'neon_mlbs2021', 'neon_moab2018', 'neon_moab2021', 
               'neon_niwo2019', 'neon_niwo2020', 'neon_nogp2021', 'neon_onaq2019', 'neon_onaq2021', 'neon_osbs2018', 'neon_osbs2019', 
               'neon_osbs2021', 'neon_puum2020', 'neon_rmnp2018', 'neon_rmnp2020', 'neon_scbi2019', 'neon_scbi2021', 'neon_serc2019', 
               'neon_serc2021', 'neon_sjer2019', 'neon_soap2018', 'neon_soap2019', 'neon_soap2021', 'neon_srer2019', 'neon_srer2021', 
               'neon_stei2019', 'neon_stei2020', 'neon_ster2021', 'neon_tall2018', 'neon_tall2019', 'neon_tall2021', 'neon_teak2021', 
               'neon_ukfs2018', 'neon_ukfs2019', 'neon_ukfs2020', 'neon_unde2019', 'neon_unde2020', 'neon_wood2021', 'neon_wref2019', 
               'neon_wref2021', 'neon_yell2018', 'neon_yell2019', 'neon_yell2020', 
               'neon_blan2022', 'neon_clbj2022', 'neon_grsm2022', 'neon_moab2022', 'neon_onaq2022', 'neon_rmnp2022', 'neon_serc2022', 
               'neon_stei2022', 'neon_steicheq2022', 'neon_ster2022', 'neon_unde2022', 'inpe_brazil31983', 'inpe_brazil31981', 
               'inpe_brazil31979', 'inpe_brazil31976', 'inpe_brazil31975', 'inpe_brazil31973', 'inpe_brazil31974', 'inpe_brazil31978']

def image_2_pdf(image_path, output_pdf_path, text, tmp_path):
    # Open the JPEG image
    image = Image.open(image_path)

    # Create a drawing object
    draw = ImageDraw.Draw(image)
  
    font_color = (0, 0, 255)  # white color
    myFont = ImageFont.truetype('FreeMono.ttf', 80)
    # Add text to the image
    draw.text((400, 60), text, font= myFont, fill=font_color)
    # Save the modified image
    image.save(tmp_path)
    # Create a PDF file
    pdf = canvas.Canvas(output_pdf_path)
    # Set the PDF dimensions based on the image size
    pdf.setPageSize((image.width, image.height))

    # Draw the image on the PDF
    pdf.drawInlineImage(tmp_path, 0, 0, width=image.width, height=image.height)

    # Save the PDF
    pdf.save()


import PyPDF2

def merge_pdfs(filepaths, output_filepath):
    # Create a PDF merger object
    pdf_merger = PyPDF2.PdfMerger()

    # Iterate through each PDF file and append it to the merger
    for filepath in filepaths:
        with open(filepath, 'rb') as pdf_file:
            pdf_merger.append(pdf_file)

    # Write the merged PDF to the output file
    with open(output_filepath, 'wb') as output_file:
        pdf_merger.write(output_file)


if __name__ == "__main__":
    
    tmp_path = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_calval_metrics/tmp.jpeg'
    if os.path.exists(tmp_path):
        # File exists, so delete it
        os.remove(tmp_path)
    # Specify the input JPEG file, output image file, output PDF file, and text to be added
    images = glob.glob('/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_calval_metrics/*.jpeg')
    for image in images:
        output_pdf_path = image[:-4] + 'pdf'
        text_to_add = os.path.basename(image)[:-5]
        image_2_pdf(image, output_pdf_path, text_to_add, tmp_path)
        print(f"Conversion complete. PDF saved to {output_pdf_path}")
    os.remove(tmp_path)
    
    print('# merge into a single pdf...')
    # Example usage:
    # List of PDF files to be merged
    pdf_files_to_merge = glob.glob('/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_calval_metrics/*.pdf')

    # get only valid sites 
    # len(VALID_SITES)
    # usa_neon_clbj2022.jpeg
    valid_pdf=[]
    for f in pdf_files_to_merge:
        base_name = os.path.basename(f)[:-4]
        # usa_neon_clbj2022.jpeg 
        split_strings = base_name.split('_')
        site1 = '_'.join(split_strings[1:])
        if site1 in VALID_SITES:
            #print(site1)
            valid_pdf.append(f)
    valid_pdf = sorted(valid_pdf)
    # Output file name for the merged PDF
    output_pdf_file = '../report/valid_sites_calval_v20240129.pdf'
    # Call the merge_pdfs function
    merge_pdfs(valid_pdf, output_pdf_file)


