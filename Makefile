.PHONY: build clean dry-run

# Build output/cv.pdf from sections/*.md using templates/my.tex.template (Steve Nguyen style).
# hint.md is reference only — edit sections/*.md, not hint.md, then run `make`.

build:
	@python3 build/build.py

dry-run:
	@python3 build/build.py --dry-run
	@echo ""
	@echo "Generated LaTeX:"
	@cat .build/cv.tex

clean:
	@rm -rf .build output
	@echo "🧹 Cleaned .build/ and output/"
