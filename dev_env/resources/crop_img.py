from importlib.resources import path
from PIL import Image
from os import listdir
from os.path import isfile, join
from pathlib import Path


if __name__ == '__main__':
    frames_path = '/home/vagrant/sample-video/frames'
    output_path = '/home/vagrant/sample-video/frames_split'
    imgs = [join(frames_path, f) for f in listdir(frames_path) if isfile(join(frames_path, f))]
    for img in imgs:
        image = Image.open(img)
        width, height = image.size
        # https://stackoverflow.com/a/58350508
        overlap = 150
        img_left_area = (0, 0, width//2 + overlap, height)
        img_right_area = (width//2 - overlap, 0, width, height)
        img_left = image.crop(img_left_area)
        img_right = image.crop(img_right_area)

        img_basename = Path(img).stem
        ext = Path(img).suffix

        outimg_basename_left = img_basename + 'l'
        outimg_basename_right = img_basename + 'r'
        outimg_filename_left = outimg_basename_left + ext
        outimg_filename_right = outimg_basename_right + ext
        outimg_fullpath_left = join(output_path, outimg_filename_left)
        outimg_fullpath_right = join(output_path, outimg_filename_right)

        img_left.save(outimg_fullpath_left)
        img_right.save(outimg_fullpath_right)
        